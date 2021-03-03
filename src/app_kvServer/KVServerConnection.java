package app_kvServer;

import static shared.communication.messages.DataTransferMessage.DataTransferMessageType.DATA_TRANSFER_FAILURE;
import static shared.communication.messages.DataTransferMessage.DataTransferMessageType.DATA_TRANSFER_REQUEST;
import static shared.communication.messages.DataTransferMessage.DataTransferMessageType.DATA_TRANSFER_SUCCESS;

import app_kvServer.data.SynchronizedKVManager;
import ecs.ECSMetadata;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.log4j.Logger;
import shared.communication.Protocol;
import shared.communication.ProtocolException;
import shared.communication.messages.DataTransferMessage;
import shared.communication.messages.ECSMessage;
import shared.communication.messages.KVMessage;
import shared.communication.messages.Message;

public class KVServerConnection implements Runnable {
  private static final Logger logger = Logger.getLogger(KVServerConnection.class);
  private final AtomicBoolean isRunning = new AtomicBoolean();
  private final Socket clientSocket;
  private final SynchronizedKVManager kvManager;
  private final InputStream input;
  private final OutputStream output;
  private final AtomicBoolean serverAcceptingClients;
  private final ECSMetadata ecsMetadata;

  public KVServerConnection(final Socket clientSocket, AtomicBoolean serverAcceptingClients)
      throws IOException {
    this.clientSocket = clientSocket;
    this.input = clientSocket.getInputStream();
    this.output = clientSocket.getOutputStream();
    this.serverAcceptingClients = serverAcceptingClients;
    this.ecsMetadata = ECSMetadata.getInstance();
    kvManager = SynchronizedKVManager.getInstance();
  }

  @Override
  public void run() {
    isRunning.set(true);
    while (isRunning.get()) {
      try {
        final Message request = Protocol.receiveMessage(input);
        if (request.getClass() == KVMessage.class) {
          final KVMessage response = handleKVMessage((KVMessage) request);
          Protocol.sendMessage(output, response);
        } else if (request.getClass() == ECSMessage.class) {
          final ECSMessage response = handleECSMessage((ECSMessage) request);
          Protocol.sendMessage(output, response);
        } else if (request.getClass() == DataTransferMessage.class) {
          final DataTransferMessage response =
              handleDataTransferMessage((DataTransferMessage) request);
          Protocol.sendMessage(output, response);
        }
      } catch (IOException | ProtocolException e) {
        logger.error("Unexpected error, dropping connection to " + clientSocket, e);
        stop();
        return;
      }
    }
  }

  private DataTransferMessage handleDataTransferMessage(DataTransferMessage request) {
    logger.info("Received data transfer request with message: " + request.getMessage());
    return kvManager.handleDataTransfer(request);
  }

  private KVMessage handleKVMessage(KVMessage request) {
    final KVMessage response;
    if (serverAcceptingClients.get()) {
      response = kvManager.handleRequest(request);
    } else {
      logger.debug("Handling KVMessage but server stopped: " + request.getKey());
      response =
          new KVMessage(
              request.getKey(),
              request.getValue(),
              KVMessage.StatusType.SERVER_STOPPED,
              "No requests can be processed at the moment");
    }
    return response;
  }

  private ECSMessage handleECSMessage(ECSMessage request) throws IOException {
    final ECSMessage reply;
    switch (request.getAction()) {
      case INIT:
        logger.info("Server INITIALIZED by ECS");
        this.serverAcceptingClients.set(false);
        this.ecsMetadata.update(request.getMetadata());
        reply = new ECSMessage(ECSMessage.ActionStatus.ACTION_SUCCESS, "INIT SUCCESS");
        break;
      case START:
        logger.info("Server STARTED by ECS");
        this.serverAcceptingClients.set(true);
        reply = new ECSMessage(ECSMessage.ActionStatus.ACTION_SUCCESS, "START SUCCESS");
        break;
      case STOP:
        logger.info("Server STOPPED by ECS");
        this.serverAcceptingClients.set(false);
        reply = new ECSMessage(ECSMessage.ActionStatus.ACTION_SUCCESS, "STOP SUCCESS");
        break;
      case SHUTDOWN:
        // TODO: Handle this action gracefully
        logger.info("Shutting down KV Server");
        reply = new ECSMessage(ECSMessage.ActionStatus.ACTION_SUCCESS, "SHUTDOWN SUCCESS");
        Protocol.sendMessage(output, reply);
        System.exit(0);
        break;
      case LOCK_WRITE:
        kvManager.setWriteEnabled(false);
        reply = new ECSMessage(ECSMessage.ActionStatus.ACTION_SUCCESS, "LOCK_WRITE SUCCESS");
        break;
      case UNLOCK_WRITE:
        kvManager.setWriteEnabled(true);
        reply = new ECSMessage(ECSMessage.ActionStatus.ACTION_SUCCESS, "UNLOCK_WRITE SUCCESS");
        break;
      case UPDATE_METADATA:
        this.ecsMetadata.update(request.getMetadata());
        reply = new ECSMessage(ECSMessage.ActionStatus.ACTION_SUCCESS, "UPDATE_METADATA SUCCESS");
        break;
      case MOVE_DATA:
        logger.info("Received a MOVE_DATA request");
        reply = doDataTransfer(request);
        break;
      default:
        throw new IllegalStateException("Unexpected value: " + request.getAction());
    }
    return reply;
  }

  private ECSMessage doDataTransfer(ECSMessage request) {
    try {
      InetAddress serverAddress =
          InetAddress.getByName(request.getDataTransferServer().getNodeHost());
      int port = request.getDataTransferServer().getNodePort();

      Socket serverSocket = new Socket(serverAddress, port);
      InputStream inputStream = serverSocket.getInputStream();
      OutputStream outputStream = serverSocket.getOutputStream();

      DataTransferMessage dataTransferMessage =
          kvManager.partitionDatabaseAndGetKeysInRange(request.getDataTransferHashRange());
      if (dataTransferMessage.getDataTransferMessageType() == DATA_TRANSFER_REQUEST) {
        Protocol.sendMessage(outputStream, dataTransferMessage);
        DataTransferMessage reply = (DataTransferMessage) Protocol.receiveMessage(inputStream);

        if (reply.getDataTransferMessageType() == DATA_TRANSFER_SUCCESS) {
          return new ECSMessage(
              ECSMessage.ActionStatus.ACTION_SUCCESS, "MOVE_DATA SUCCESS: " + reply.getMessage());
        } else if (reply.getDataTransferMessageType() == DATA_TRANSFER_FAILURE) {
          // Revert database changes
          logger.info(
              "Data transfer failed on remote: " + reply.getMessage() + " reverting DB changes");
          kvManager.handleDataTransfer(dataTransferMessage);

          return new ECSMessage(
              ECSMessage.ActionStatus.ACTION_FAILED,
              "MOVE_DATA REMOTE FAILURE: " + reply.getMessage());
        }
      } else if (dataTransferMessage.getDataTransferMessageType() == DATA_TRANSFER_FAILURE) {
        return new ECSMessage(
            ECSMessage.ActionStatus.ACTION_FAILED,
            "MOVE_DATA LOCAL FAILURE: " + dataTransferMessage.getMessage());
      }

    } catch (UnknownHostException e) {
      return new ECSMessage(
          ECSMessage.ActionStatus.ACTION_FAILED,
          "MOVE_DATA FAILURE: Bad Server Host " + e.toString());
    } catch (IOException e) {
      return new ECSMessage(
          ECSMessage.ActionStatus.ACTION_FAILED, "MOVE_DATA FAILURE: " + e.toString());
    } catch (ProtocolException e) {
      return new ECSMessage(
          ECSMessage.ActionStatus.ACTION_FAILED,
          "MOVE_DATA FAILURE: Protocol Exception " + e.toString());
    }
    return null;
  }

  public void stop() {
    isRunning.set(false);
    if (!clientSocket.isClosed()) {
      try {
        clientSocket.close();
      } catch (IOException e) {
        logger.error("Error closing " + clientSocket, e);
      }
    }
  }
}
