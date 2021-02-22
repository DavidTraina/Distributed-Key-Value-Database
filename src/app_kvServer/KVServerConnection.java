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
        final Message request = Protocol.receiveMessage(input);

        if (request.getClass() == KVMessage.class) {
          final KVMessage response = kvManager.handleRequest((KVMessage) request);
          Protocol.sendMessage(output, response);
        } else if (request.getClass() == ECSMessage.class) {
          // TODO: Handle ECS commands
        }
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
