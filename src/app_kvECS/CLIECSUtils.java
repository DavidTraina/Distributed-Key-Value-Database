package app_kvECS;

import ecs.ECSMetadata;
import ecs.ECSNode;
import org.apache.log4j.Logger;

public class CLIECSUtils {
  private static final Logger logger = Logger.getLogger(CLIECSUtils.class);
  private static final String PROMPT = "ECS> ";

  protected static void printError(String error) {
    System.out.println(PROMPT + error);
  }

  protected static void printMessage(String message) {
    System.out.println(PROMPT + message);
  }

  protected static void printRequestResult(boolean result) {
    if (result) {
      CLIECSUtils.printMessage("Request succeeded");
    } else {
      CLIECSUtils.printMessage("Request failed");
    }
  }

  public static String[] tokenize(String cmdLine) {
    if (cmdLine == null) {
      System.out.println();
      return new String[] {"quit"};
    }
    cmdLine = cmdLine.trim();
    return cmdLine.split("\\s+", -1);
  }

  protected static void printHelp() {
    StringBuilder sb = new StringBuilder();
    sb.append(PROMPT).append("ECS APP HELP (Usage):\n");
    sb.append(PROMPT);
    sb.append("::::::::::::::::::::::::::::::::");
    sb.append("::::::::::::::::::::::::::::::::\n");
    sb.append(PROMPT).append("start");
    sb.append(
        "\t starts the storage service by calling start() on all KVServer instances that"
            + " participate in the service\n");
    sb.append(PROMPT).append("stop");
    sb.append(
        "\t stops the service; all participating KVServers are stopped for processing client"
            + " requests but the processes remain running \n");
    sb.append(PROMPT).append("shutDown");
    sb.append("\t stops all server instances and exits the remote processes \n");
    sb.append(PROMPT).append("addNode");
    sb.append(
        "\t creates a new KVServer and add it to the storage service at an arbitrary position \n");
    sb.append(PROMPT).append("removeNode <nodeId>");
    sb.append("\t removes a server from the storage service at an arbitrary position \n");

    sb.append(PROMPT).append("quit ");
    sb.append(
        "\t\t\t shuts down all running servers and exits the program (can also press ctrl+d)");
    System.out.println(sb.toString());
  }

  protected static void printMetadata(ECSMetadata metadata) {
    StringBuilder sb = new StringBuilder();
    if (metadata == null) {
      sb.append(PROMPT).append("\nMetadata is not initialized!");
    } else {
      sb.append(PROMPT).append("Metadata:\n");
      sb.append(PROMPT);
      sb.append("::::::::::::::::::::::::::::::::\n");
      sb.append("Current number of nodes: ").append(metadata.getMetadata().size()).append("\n");
      for (ECSNode node : metadata.getMetadata()) {
        sb.append("--------------------------------\n");
        sb.append(PROMPT).append("Node ").append(node.getNodeName()).append("\n");
        sb.append(PROMPT)
            .append("Hash range from ")
            .append(node.getLowerRange())
            .append(" to ")
            .append(node.getNodeHash())
            .append("\n");
      }
      System.out.println(sb.toString());
    }
  }

  protected static void printPrompt() {
    System.out.print(PROMPT);
  }
}
