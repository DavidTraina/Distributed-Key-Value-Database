package shared.communication;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.log4j.Logger;
import shared.communication.messages.KVMessage;
import shared.communication.messages.KVMessageException;

public class Protocol {
  private static final Logger logger = Logger.getLogger(Protocol.class);
  private static final int BUFFER_SIZE = 1024;
  private static final int DROP_SIZE = 128 * BUFFER_SIZE;
  private static final char LINE_FEED = 0x0A;
  private static final char RETURN = 0x0D;
  private static final byte[] ctrBytes = new byte[] {LINE_FEED, RETURN};

  public static void sendMessage(final OutputStream output, KVMessage message) throws IOException {
    byte[] bytes = message.serialize();
    byte[] messageBytes = new byte[bytes.length + ctrBytes.length];

    System.arraycopy(bytes, 0, messageBytes, 0, bytes.length);
    System.arraycopy(ctrBytes, 0, messageBytes, bytes.length, ctrBytes.length);

    output.write(messageBytes);
    output.flush();
  }

  public static KVMessage receiveMessage(final InputStream input)
      throws IOException, ProtocolException {
    int index = 0;
    byte[] msgBytes = null, tmp;
    byte[] bufferBytes = new byte[BUFFER_SIZE];

    /* read first char from stream */
    byte read = (byte) input.read();
    boolean reading = true;

    while (read != 13 && reading) {
      /* carriage return */
      /* if buffer filled, copy to msg array */
      if (index == BUFFER_SIZE) {
        if (msgBytes == null) {
          tmp = new byte[BUFFER_SIZE];
          System.arraycopy(bufferBytes, 0, tmp, 0, BUFFER_SIZE);
        } else {
          tmp = new byte[msgBytes.length + BUFFER_SIZE];
          System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
          System.arraycopy(bufferBytes, 0, tmp, msgBytes.length, BUFFER_SIZE);
        }

        msgBytes = tmp;
        bufferBytes = new byte[BUFFER_SIZE];
        index = 0;
      }

      bufferBytes[index] = read;
      index++;

      /* stop reading is DROP_SIZE is reached */
      if (msgBytes != null && msgBytes.length + index >= DROP_SIZE) {
        reading = false;
      }

      /* read next char from stream */
      read = (byte) input.read();
    }

    if (msgBytes == null) {
      tmp = new byte[index];
      System.arraycopy(bufferBytes, 0, tmp, 0, index);
    } else {
      tmp = new byte[msgBytes.length + index];
      System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
      System.arraycopy(bufferBytes, 0, tmp, msgBytes.length, index);
    }

    msgBytes = tmp;

    // Check if stream is closed (read returns -1)
    if (read == -1) {
      throw new ProtocolException("Connection closed by the other side!");
    }

    /* build final String */
    KVMessage message = null;
    try {
      byte[] actualBytes = new byte[msgBytes.length - ctrBytes.length + 1];
      System.arraycopy(msgBytes, 0, actualBytes, 0, actualBytes.length);

      message = KVMessage.deserialize(actualBytes);
      logger.info("Receive message status: '" + message.getStatus() + "'");
    } catch (KVMessageException e) {
      logger.error("Message failed to deserialize!", e);
    }
    return message;
  }
}
