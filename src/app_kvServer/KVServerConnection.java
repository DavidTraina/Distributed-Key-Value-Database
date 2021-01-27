package app_kvServer;

import app_kvServer.data.SynchronizedKVManager;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.log4j.Logger;
import shared.communication.Protocol;
import shared.exceptions.ConnectionLostException;
import shared.messages.KVMessage;

public class KVServerConnection implements Runnable {
  private static final Logger logger = Logger.getRootLogger();
  private final AtomicBoolean isRunning = new AtomicBoolean();
  private final Socket clientSocket;
  private final SynchronizedKVManager kvManager;
  private InputStream input;
  private OutputStream output;

  public KVServerConnection(final Socket clientSocket) throws IOException {
    this.clientSocket = clientSocket;
    this.input = clientSocket.getInputStream();
    this.output = clientSocket.getOutputStream();
    kvManager = SynchronizedKVManager.getInstance();
  }

  @Override
  public void run() {
    isRunning.set(true);
    while (isRunning.get()) {
      // TODO: handle future deserialize exception and return failure response
      try {
        final KVMessage request = Protocol.receiveMessage(input);
        final KVMessage response = kvManager.handleRequest(request);
        Protocol.sendMessage(output, response);
      } catch (IOException | ConnectionLostException e) {
        logger.error("Connect lost! Stopping thread");
        stop();
      }
    }
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
