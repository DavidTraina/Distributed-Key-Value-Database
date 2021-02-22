package client;

import ecs.ECSMetadata;
import ecs.ECSNode;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import org.apache.log4j.Logger;
import shared.communication.Protocol;
import shared.communication.ProtocolException;
import shared.communication.messages.KVMessage;

public class KVStore implements KVCommInterface {
  private static final Logger logger = Logger.getLogger(KVStore.class);
  private InetAddress address;
  private int port;
  private Socket clientSocket;
  private ECSMetadata metadata = null;
  private OutputStream output;
  private InputStream input;

  /**
   * Initialize KVStore with address and port of KVServer
   *
   * @param address the address of the KVServer
   * @param port the port of the KVServer
   */
  public KVStore(InetAddress address, int port) {
    this.address = address;
    this.port = port;
  }

  @Override
  public void connect() throws KVStoreException {
    if (this.metadata != null) {
      this.metadata.clear();
    }
    this.metadata = null;
    createConnection();
  }

  public void createConnection() throws KVStoreException {
    try {
      this.clientSocket = new Socket(address, port);
      this.output = clientSocket.getOutputStream();
      this.input = clientSocket.getInputStream();
    } catch (IOException e) {
      logger.error("Could not open connection successfully ", e);
      throw new KVStoreException("Error on connect: " + e.getMessage());
    }
  }

  @Override
  public void disconnect() throws KVStoreException {
    logger.info("Tearing down the connection ...");
    if (this.clientSocket != null) {
      if (!this.clientSocket.isClosed()) {
        try {
          this.clientSocket.close();
        } catch (IOException e) {
          logger.error("Could not close connection to " + clientSocket + " successfully", e);
          throw new KVStoreException("Error on disconnect: " + e.getLocalizedMessage());
        }
      }
      this.clientSocket = null;
      logger.info("Connection closed!");
    }
  }

  @Override
  public KVMessage put(String key, String value) throws KVStoreException {
    if (this.metadata != null) {
      reconnectToCorrectServer(key);
    }
    // Sending the PUT request
    KVMessage message = new KVMessage(key, value, KVMessage.StatusType.PUT);
    try {
      Protocol.sendMessage(output, message);
      logger.info("PUT request for '" + key + ":" + value + "'");

      // Receiving response to the PUT request
      KVMessage responseMessage = Protocol.receiveMessage(input);
      if (responseMessage.getStatus() != KVMessage.StatusType.NOT_RESPONSIBLE) {
        return responseMessage;
      } else {
        assert (responseMessage.getMetadata() != null);
        this.metadata = responseMessage.getMetadata(); // TODO check that equal??
        return put(key, value);
      }

    } catch (IOException | ProtocolException e) {
      logger.error("Failed to receive PUT request");
      throw new KVStoreException("Error on PUT: " + e.getLocalizedMessage() + " for " + key);
    }
  }

  @Override
  public KVMessage get(String key) throws KVStoreException {
    if (this.metadata != null) {
      reconnectToCorrectServer(key);
    }
    // Sending the GET request
    KVMessage message = new KVMessage(key, null, KVMessage.StatusType.GET);
    try {
      Protocol.sendMessage(output, message);
      logger.info("GET request for '" + key + "'");

      // Receiving response to the GET request
      KVMessage responseMessage = Protocol.receiveMessage(input);
      if (responseMessage.getStatus() != KVMessage.StatusType.NOT_RESPONSIBLE) {
        return responseMessage;
      } else {
        assert (responseMessage.getMetadata() != null);
        this.metadata = responseMessage.getMetadata(); // TODO check that equal??
        return get(key);
      }

    } catch (IOException | ProtocolException e) {
      logger.error("Failed to receive GET request");
      throw new KVStoreException("Error on GET: " + e.getLocalizedMessage() + " for " + key);
    }
  }

  private void reconnectToCorrectServer(String key) throws KVStoreException {
    ECSNode server = metadata.getNodeBasedOnKey(key);
    try {
      this.address = InetAddress.getByName(server.getNodeHost());
    } catch (UnknownHostException e) {
      logger.error("Could not reconnect because host not identified");
      throw new KVStoreException("Error on GET: " + e.getLocalizedMessage() + " for " + key);
    }
    this.port = server.getNodePort();

    disconnect();
    connect();
  }
}
