package ecs;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import org.apache.log4j.Logger;
import shared.communication.Protocol;
import shared.communication.ProtocolException;
import shared.communication.messages.ECSMessage;
import shared.communication.messages.Message;
import shared.communication.messages.MetadataUpdateMessage;
import shared.communication.security.Hashing;

public class ECSUtils {
  private static final Logger logger = Logger.getLogger(ECSUtils.class);

  public static boolean checkIfKeyBelongsInRange(String key, String[] ends) {
    String hash = Hashing.calculateMD5Hash(key);
    // the node is not wrapping
    if (ends[0].compareTo(ends[1]) < 0) { // left < right
      return ends[0].compareTo(hash) <= 0 && ends[1].compareTo(hash) > 0;
    } else { // left >= right
      return ends[1].compareTo(hash) >= 0 || ends[0].compareTo(hash) < 0;
    }
  }

  public static ArrayList<ECSNode> parseConfigFile(String filepath)
      throws IllegalArgumentException, IOException {
    BufferedReader configReader = new BufferedReader(new FileReader(filepath));
    String currentLine;
    ArrayList<ECSNode> availableNodes = new ArrayList<>();
    while ((currentLine = configReader.readLine()) != null) {
      String[] tokens = currentLine.split(" ");
      if (tokens.length != 2) {
        throw new IllegalArgumentException("Config file is invalid.");
      }
      String address = tokens[0];
      if (address.equals("localhost")) address = "127.0.0.1";
      int port = Integer.parseInt(tokens[1]);
      availableNodes.add(new ECSNode(address, port));
    }
    return availableNodes;
  }

  public static synchronized boolean sendECSMessageToNode(ECSNode node, ECSMessage message) {
    try {
      Socket clientSocket = new Socket(node.getNodeHost(), node.getNodePort());

      // Wait maximum 200ms for a response
      clientSocket.setSoTimeout(2000);

      OutputStream output = clientSocket.getOutputStream();
      InputStream input = clientSocket.getInputStream();
      Protocol.sendMessage(output, message);
      logger.debug("Sent ECSMessage: " + message.getAction() + " to: " + clientSocket);
      Message response;
      while ((response = Protocol.receiveMessage(input)).getClass() != ECSMessage.class) {
        assert (response.getClass() == MetadataUpdateMessage.class);
        logger.info("received MetadataUpdateMessage message " + response);
      }
      ECSMessage.ActionStatus responseStatus = ((ECSMessage) response).getStatus();
      logger.debug("Response ECSMessage: " + responseStatus);
      clientSocket.close();
      return (ECSMessage.ActionStatus.ACTION_SUCCESS == responseStatus);
    } catch (SocketTimeoutException e) {
      logger.error("Socket timeout on read: server took too long to respond");
      return false;
    } catch (IOException | ProtocolException e) {
      logger.error("Socket exception: ", e);
      return false;
    }
  }
}
