package app_kvServer;

import app_kvServer.data.SynchronizedKVManager;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.log4j.Logger;
import shared.messages.KVMessage;

public class KVServerConnection implements Runnable {
  private static final Logger logger = Logger.getRootLogger();
  private static final int BUFFER_SIZE = 1024;
  private static final int DROP_SIZE = 128 * BUFFER_SIZE;
  private final AtomicBoolean isRunning = new AtomicBoolean();
  private final Socket clientSocket;
  private final SynchronizedKVManager kvManager;
  private InputStream input;
  private OutputStream output;

  public KVServerConnection(final Socket clientSocket) {
    this.clientSocket = clientSocket;
    kvManager = SynchronizedKVManager.getInstance();
  }

  @Override
  public void run() {
    isRunning.set(true);
    // TODO(Nekhil) socket communication loop
    // Outline of kvManager interaction
    final byte[] requestBytes = null;
    final KVMessage request = KVMessage.deserialize(requestBytes);
    // TODO handle future deserialize exception and return failure response
    final KVMessage response = kvManager.handleRequest(request);
    final byte[] responseBytes = response.serialize();
  }

  public void stop() {
    isRunning.set(false);
    if (!clientSocket.isClosed()) {
      try {
        clientSocket.close();
      } catch (IOException e) {
        logger.error("Error closing client socket: " + clientSocket.toString() + "\n", e);
      }
    }
  }
}
