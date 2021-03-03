package client;

import ecs.ECSMetadata;
import ecs.ECSNode;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashMap;
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
  private final HashMap<String, Socket> socketCache = new HashMap<>();

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
      KVMessage responseMessage = (KVMessage) Protocol.receiveMessage(input);
      if (responseMessage.getStatus() != KVMessage.StatusType.NOT_RESPONSIBLE) {
        return responseMessage;
      } else {
        assert (responseMessage.getMetadata() != null);
        this.metadata = responseMessage.getMetadata(); // TODO check that equal??
        logger.debug("Re-requesting PUT to another server");
        return put(key, value);
      }
    } catch (IOException e) {
      logger.error("Failed to receive PUT response");
      logger.info("All in-memory connections " + metadata);
      // metadata is stale, discard
      metadata = null;
      socketCache.clear();
      throw new KVStoreException("Error on PUT: " + e.getLocalizedMessage() + " for " + key);
    } catch (ProtocolException e) {
      logger.error("Failed to receive GET response " + e);
      if (metadata != null) {
        KVStoreException ex =
            new KVStoreException(
                "Problem communicating to server try connecting to another server from these"
                    + " options: "
                    + this.metadata.toString());
        // metadata is stale, discard
        metadata = null;
        socketCache.clear();
        throw ex;
      }
      throw new KVStoreException(
          "Problem communicating to server try connecting to another server in cluster");
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
      KVMessage responseMessage = (KVMessage) Protocol.receiveMessage(input);
      if (responseMessage.getStatus() != KVMessage.StatusType.NOT_RESPONSIBLE) {
        return responseMessage;
      } else {
        assert (responseMessage.getMetadata() != null);
        this.metadata = responseMessage.getMetadata();
        logger.debug("Re-requesting GET to another server");
        return get(key);
      }
    } catch (IOException e) {
      logger.error("Failed to receive GET response");
      // metadata is stale, discard
      metadata = null;
      socketCache.clear();
      throw new KVStoreException("Error on GET: " + e.getLocalizedMessage() + " for " + key);
    } catch (ProtocolException e) {
      logger.error("Failed to receive GET response " + e);
      if (metadata != null) {
        KVStoreException ex =
            new KVStoreException(
                "Problem communicating to server try connecting to another server from these"
                    + " options: "
                    + this.metadata.toString());
        // metadata is stale, discard
        metadata = null;
        socketCache.clear();
        throw ex;
      }
      socketCache.clear();
      throw new KVStoreException(
          "Problem communicating to server try connecting to another server in cluster");
    }
  }

  private void reconnectToCorrectServer(final String key) throws KVStoreException {
    ECSNode server = metadata.getNodeBasedOnKey(key);
    if (server == null) {
      throw new KVStoreException("Please reconnect to a server, internal configuration problems");
    }
    if (socketCache.containsKey(server.getNodeName())) {
      this.clientSocket = socketCache.get(server.getNodeName());
      try {
        this.output = clientSocket.getOutputStream();
        this.input = clientSocket.getInputStream();
      } catch (IOException e) {
        logger.error("Could not open connection successfully ", e);
        throw new KVStoreException("Error on connect: " + e.getMessage());
      }
    } else {
      try {
        this.address = InetAddress.getByName(server.getNodeHost());
      } catch (UnknownHostException e) {
        logger.error("Could not reconnect because host not identified");
        throw new KVStoreException("Error: " + e.getLocalizedMessage() + " for " + key);
      }
      this.port = server.getNodePort();

      connect();
      socketCache.put(server.getNodeName(), this.clientSocket);
    }
  }
}
