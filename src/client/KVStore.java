package client;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import org.apache.log4j.Logger;
import shared.communication.Protocol;
import shared.exceptions.ConnectionLostException;
import shared.messages.KVMessage;

public class KVStore implements KVCommInterface {
  private static final Logger logger = Logger.getRootLogger();
  private final InetAddress address;
  private final int port;
  private Socket clientSocket;
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
  public void connect() throws IOException {
    this.clientSocket = new Socket(address, port);
    this.output = clientSocket.getOutputStream();
    this.input = clientSocket.getInputStream();
  }

  @Override
  public void disconnect() {
    logger.info("Tearing down the connection ...");
    if (this.clientSocket != null) {
      if (!this.clientSocket.isClosed()) {
        try {
          this.clientSocket.close();
        } catch (IOException e) {
          logger.error("Could not close connection successfully", e);
        }
      }
      this.clientSocket = null;
      logger.info("Connection closed!");
    }
  }

  @Override
  public KVMessage put(String key, String value) throws IOException, ConnectionLostException {
    // Sending the PUT request
    KVMessage message = new KVMessage(key, value, KVMessage.StatusType.PUT);
    Protocol.sendMessage(output, message);
    logger.info("PUT request for '" + key + ":" + value + "'");

    // Receiving response to the PUT request
    KVMessage responseMessage = Protocol.receiveMessage(input);
    return responseMessage;
  }

  @Override
  public KVMessage get(String key) throws IOException, ConnectionLostException {
    // Sending the GET request
    KVMessage message = new KVMessage(key, null, KVMessage.StatusType.GET);
    Protocol.sendMessage(output, message);
    logger.info("GET request for '" + key + "'");

    // Receiving response to the GET request
    KVMessage responseMessage = Protocol.receiveMessage(input);
    return responseMessage;
  }
}
