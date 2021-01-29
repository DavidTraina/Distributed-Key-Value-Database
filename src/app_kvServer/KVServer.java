package app_kvServer;

import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.log4j.Logger;

public class KVServer implements Runnable {

  private static final Logger logger = Logger.getLogger(KVServer.class);

  private final int port;
  private String address = "";
  private final AtomicBoolean isRunning = new AtomicBoolean();
  private volatile ServerSocket serverSocket;

  public KVServer(final int port) {
    this.port = port;
  }

  @Override
  public void run() {
    initializeServerSocket();
    while (isRunning.get()) {
      try {
        final Socket clientSocket = serverSocket.accept();
        new Thread(new KVServerConnection(clientSocket), "Conn Thread: " + clientSocket).start();
        logger.info("New connection to " + clientSocket + " accepted.");
      } catch (IOException e) {
        if (!isRunning.get()) {
          logger.debug("Failed to accept client connection because server was stopped.\n", e);
        } else {
          logger.error("Unknown error accepting client connection. Continuing accept loop. \n", e);
        }
      }
    }
    logger.info("Server stopped");
  }

  public void stop() {
    logger.info("Stopping KVServer on " + serverSocket);
    if (serverSocket == null) {
      logger.error("Server is uninitialized.");
      throw new AssertionError("Server is uninitialized.");
    }
    isRunning.set(false);
    if (!serverSocket.isClosed()) {
      try {
        serverSocket.close();
      } catch (IOException e) {
        logger.error("Error! Unable to close " + serverSocket, e);
      }
    }
  }

  private void initializeServerSocket() {
    logger.info("Initializing server ...");
    if (serverSocket != null) {
      logger.error("Server on " + serverSocket + " is already initialized.");
      throw new AssertionError("Server already initialized");
    }
    try {
      serverSocket = new ServerSocket(port);
    } catch (BindException e) {
      logger.error("Error opening server socket: Port " + port + " is already bound!", e);
      return;
    } catch (IOException e) {
      logger.error("Error opening server socket: ", e);
      return;
    }
    String newThreadName = "Server Thread: " + serverSocket;
    logger.info("Renaming current thread to '" + newThreadName + "'");
    Thread.currentThread().setName(newThreadName);
    logger.info("Server listening on " + serverSocket);
    isRunning.set(true);
  }
}
