package testing;

import ecs.ECSNode;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.security.SecureRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import shared.communication.Protocol;
import shared.communication.ProtocolException;
import shared.communication.messages.Message;

public class TestUtils {
  // Copied from stackoverflow:
  // https://stackoverflow.com/questions/39222044/generate-random-string-in-java
  public static String createRandomCode(int codeLength, String id) {
    return new SecureRandom()
        .ints(codeLength, 0, id.length())
        .mapToObj(id::charAt)
        .map(Object::toString)
        .collect(Collectors.joining());
  }

  public static void waitForSeconds(int seconds) {
    try {
      TimeUnit.SECONDS.sleep(seconds);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  public static Message sendMessageToNode(ECSNode node, Message message)
      throws IOException, ProtocolException {
    Socket clientSocket = new Socket(node.getNodeHost(), node.getNodePort());
    clientSocket.setSoTimeout(2000);

    OutputStream output = clientSocket.getOutputStream();
    InputStream input = clientSocket.getInputStream();
    Protocol.sendMessage(output, message);
    Message response;
    response = Protocol.receiveMessage(input);
    return response;
  }
}
