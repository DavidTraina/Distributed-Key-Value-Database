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
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.log4j.Logger;
import shared.communication.Protocol;
import shared.communication.ProtocolException;
import shared.communication.messages.*;
import shared.communication.security.Verifier;
import shared.communication.security.encryption.EncryptionException;

public class KVServerConnection implements Runnable {
  private static final Logger logger = Logger.getLogger(KVServerConnection.class);
  private final AtomicBoolean isRunning = new AtomicBoolean();
  private final Socket clientSocket;
  private final SynchronizedKVManager kvManager;
  private final InputStream input;
  private final OutputStream output;
  private final AtomicBoolean serverAcceptingClients;
  private final ECSMetadata ecsMetadata;
  private final LinkedBlockingQueue<KVMessage> replicationQueue;
  private Set<String> seenECSIDs;

  public KVServerConnection(
      final Socket clientSocket,
      AtomicBoolean serverAcceptingClients,
      final LinkedBlockingQueue<KVMessage> replicationQueue,
      final Set<String> seenECSIDs)
      throws IOException {
    this.clientSocket = clientSocket;
    this.input = clientSocket.getInputStream();
    this.output = clientSocket.getOutputStream();
    this.serverAcceptingClients = serverAcceptingClients;
    this.ecsMetadata = ECSMetadata.getInstance();
    this.kvManager = SynchronizedKVManager.getInstance();
    this.replicationQueue = replicationQueue;
    this.seenECSIDs = seenECSIDs;
  }

  @Override
  public void run() {
    isRunning.set(true);
    while (isRunning.get()) {
      try {
        final Message request = Protocol.receiveMessage(input);
        Message response;
        if (request.getClass() == KVMessage.class) {
          KVMessage kvRequest = (KVMessage) request;
          if (verifyKVMessageFromClient(kvRequest)) {
            response = handleClientRequest(kvRequest);
          } else {
            response =
                new KVMessage(
                    kvRequest.getKey(),
                    null,
                    KVMessage.StatusType.AUTH_FAILED,
                    kvRequest.getRequestId());
          }
        } else if (request.getClass() == ReplicationMessage.class) {
          KVMessage kvRequest = ((ReplicationMessage) request).getMessage();
          if (verifyKVMessageFromClient(kvRequest)) {
            response = kvManager.handleServerRequest(kvRequest);
          } else {
            response =
                new KVMessage(
                    kvRequest.getKey(),
                    null,
                    KVMessage.StatusType.AUTH_FAILED,
                    kvRequest.getRequestId());
          }
        } else if (request.getClass() == MetadataUpdateMessage.class) {
          response =
              new MetadataUpdateMessage(
                  ecsMetadata, ((MetadataUpdateMessage) request).getRequestId());
        } else if (request.getClass() == ECSMessage.class) {
          ECSMessage ecsRequest = (ECSMessage) request;
          if (verifyECSMessageFromServer(ecsRequest)) {
            ECSMessage ecsResponse = handleECSMessage((ECSMessage) request);
            if (ecsResponse.getStatus() == ECSMessage.ActionStatus.ACTION_SUCCESS)
              seenECSIDs.add(ecsRequest.getMAC());
            response = ecsResponse;
          } else {
            response =
                new ECSMessage(
                    ECSMessage.ActionStatus.ACTION_FAILED, "Invalid origin or corrupted data");
          }
        } else if (request.getClass() == DataTransferMessage.class) {
          DataTransferMessage DTRequest = (DataTransferMessage) request;
          ECSMessage ecsRequest = DTRequest.getECSMessage();
          if (verifyECSMessageFromServer(ecsRequest)) {
            DataTransferMessage dataTransferMessage =
                handleDataTransferMessage((DataTransferMessage) request);
            if (dataTransferMessage.getDataTransferMessageType() == DATA_TRANSFER_SUCCESS)
              seenECSIDs.add(ecsRequest.getMAC());
            response = dataTransferMessage;
          } else {
            response =
                new DataTransferMessage(
                    DATA_TRANSFER_FAILURE,
                    "Invalid origin or corrupted data" + DTRequest.getDataTransferMessageType(),
                    DTRequest.getECSMessage());
          }
        } else {
          logger.error("Unknown request type: " + request.getClass());
          continue;
        }
        send(response);
      } catch (IOException | ProtocolException e) {
        logger.error("Unexpected error, dropping connection to " + clientSocket, e);
        stop();
        return;
      }
    }
  }

  private boolean verifyKVMessageFromClient(KVMessage message) {
    ECSMetadata metadata = ECSMetadata.getInstance();
    boolean checks =
        message.getSenderID() != null && metadata.getNodeBasedOnName(message.getSenderID()) == null;
    try {
      return checks && Verifier.verifyKVMessageMAC(message);
    } catch (EncryptionException e) {
      logger.error("Asymmetric Crypto Error: " + e.getLocalizedMessage());
      return false;
    }
  }

  private boolean verifyECSMessageFromServer(ECSMessage message) {
    try {
      if (!message.getSenderID().equals("ecs")) {
        logger.info("ECS Message not from the right origin");
        return false;
      }
      if (seenECSIDs.contains(message.getMAC())) {
        logger.error("ECS Message MAC seen before, suspected replay attack");
        return false;
      }
      if (!Verifier.verifyECSMessageMAC(message)) {
        logger.info("ECS Message MAC not verified successfully");
        return false;
      }
    } catch (EncryptionException e) {
      logger.error("Asymmetric Crypto Error: " + e.getLocalizedMessage());
      return false;
    }
    return true;
  }

  private void send(Message msg) throws IOException {
    synchronized (output) {
      Protocol.sendMessage(output, msg);
    }
  }

  private DataTransferMessage handleDataTransferMessage(DataTransferMessage request) {
    logger.info("Received data transfer request with message: " + request.getMessage());
    if (request.getDataTransferMessageType() == DATA_TRANSFER_REQUEST) {
      logger.debug("Data transfer message with payload: " + request.getPayload().toString());
    }
    DataTransferMessage response = kvManager.handleDataTransfer(request);
    logger.info("Completed data transfer request with message: " + response.getMessage());
    return response;
  }

  private KVMessage handleClientRequest(KVMessage request) {
    final KVMessage response;
    if (serverAcceptingClients.get()) {
      response = kvManager.handleClientRequest(request);
      if (request.getStatus() == KVMessage.StatusType.PUT
          && (response.getStatus() == KVMessage.StatusType.PUT_UPDATE
              || response.getStatus() == KVMessage.StatusType.PUT_SUCCESS
              || response.getStatus() == KVMessage.StatusType.DELETE_SUCCESS)) {
        replicationQueue.add(request);
        logger.debug(
            "Added "
                + request.getKey()
                + " to replication queue, current length: "
                + replicationQueue.size());
      }
    } else {
      logger.debug("Handling KVMessage but server stopped: " + request.getKey());
      response =
          new KVMessage(
              request.getKey(),
              request.getValue(),
              KVMessage.StatusType.SERVER_STOPPED,
              request.getRequestId());
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
        send(reply);
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
        logger.info("Completed MOVE_DATA request with result: " + reply.getMessage());
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

      Socket dataTransferSock = new Socket(serverAddress, port);
      OutputStream outputStream = dataTransferSock.getOutputStream();

      DataTransferMessage dataTransferMessage =
          kvManager.partitionDatabaseAndGetKeysInRange(request, request.getDataTransferHashRange());
      if (dataTransferMessage.getDataTransferMessageType() == DATA_TRANSFER_REQUEST) {
        Protocol.sendMessage(outputStream, dataTransferMessage);
        logger.info("listening on: " + dataTransferSock);
        DataTransferMessage reply =
            (DataTransferMessage) Protocol.receiveMessage(dataTransferSock.getInputStream());
        logger.info("Got DataTransferMessage reply: " + reply.getMessage());

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
      return new ECSMessage(
          ECSMessage.ActionStatus.ACTION_FAILED, "Unexpected data transfer message type");
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
