package client;

import org.apache.log4j.Logger;
import shared.messages.KVMessage;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Set;

public class KVStore implements KVCommInterface {
  private static final int BUFFER_SIZE = 1024;
  private static final int DROP_SIZE = 1024 * BUFFER_SIZE;
  private static final Logger logger = Logger.getRootLogger();
  private Set<ClientSocketListener> listeners;
  private boolean running;
  private Socket clientSocket;
  private OutputStream output;
  private InputStream input;
  /**
   * Initialize KVStore with address and port of KVServer
   *
   * @param address the address of the KVServer
   * @param port the port of the KVServer
   */
  public KVStore(String address, int port) {
    // TODO Auto-generated method stub
  }

  @Override
  public void connect() throws Exception {
    // TODO Auto-generated method stub
  }

  @Override
  public void disconnect() {
    // TODO Auto-generated method stub
  }

  @Override
  public KVMessage put(String key, String value) throws Exception {
    // Sending the PUT request
    KVMessage message = new KVMessage(key, value, KVMessage.StatusType.PUT);
    byte[] messageBytes = message.serialize();
    output.write(messageBytes, 0, messageBytes.length);
    output.flush();
    logger.info("PUT request for '" + key + ":" + value + "'");

    // Receiving response to the PUT request
    return message;
  }

  @Override
  public KVMessage get(String key) throws Exception {
    // Sending the GET request
    KVMessage message = new KVMessage(key, null, KVMessage.StatusType.GET);
    byte[] messageBytes = message.serialize();
    output.write(messageBytes, 0, messageBytes.length);
    output.flush();
    logger.info("GET request for '" + key + "'");

    // Receiving response to the GET request
    return message;
  }
}
