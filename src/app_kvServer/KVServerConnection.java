package app_kvServer;

import app_kvServer.data.SynchronizedKVManager;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.log4j.Logger;
import shared.communication.Protocol;
import shared.communication.ProtocolException;
import shared.messages.KVMessage;

public class KVServerConnection implements Runnable {
  private static final Logger logger = Logger.getLogger(KVServerConnection.class);
  private final AtomicBoolean isRunning = new AtomicBoolean();
  private final Socket clientSocket;
  private final SynchronizedKVManager kvManager;
  private final InputStream input;
  private final OutputStream output;

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
      try {
        final KVMessage request = Protocol.receiveMessage(input);
        final KVMessage response = kvManager.handleRequest(request);
        Protocol.sendMessage(output, response);
      } catch (IOException | ProtocolException e) {
        logger.error("Unexpected error, dropping connection to " + clientSocket, e);
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
        logger.error("Error closing " + clientSocket, e);
      }
    }
  }
}
