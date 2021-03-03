package app_kvECS;

import app_kvServer.data.cache.CacheStrategy;
import ecs.ECSMetadata;
import ecs.ECSNode;
import ecs.ECSUtils;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import logger.LogSetup;
import org.apache.log4j.Level;

public class ECSCLI {
  private boolean stop = false;
  private static ECSClient ecsClient;

  // m - number of nodes
  // zookeeper hostname
  // zookeeper port
  // cache strategy
  // cache size
  public static void main(String[] args) {
    try {
      new LogSetup("logs/ecs.log", Level.INFO, false);
    } catch (IOException e) {
      System.out.println("Error! Unable to initialize logger!");
      e.printStackTrace();
      return;
    }

    if (args.length != 4 && args.length != 6) {
      System.out.println("Incorrect arguments for ECS. \n\n");
      System.out.println("To initialize the ECS service please input the following: \n");
      System.out.println(
          "<config file name> <number of nodes> <zookeeper ip> <zookeeper port> <cache strategy>"
              + " <cache size> \n");
      System.out.println("Using cache is optional. In case you don't want any cache issue: \n");
      System.out.println("<config file name> <number of nodes> <zookeeper ip> <zookeeper port>\n");
      return;
    }

    ArrayList<ECSNode> availableNodes;

    String configFile = args[0];
    try {
      availableNodes = ECSUtils.parseConfigFile(configFile);
    } catch (Exception e) {
      System.out.println("Invalid config file given.\n");
      return;
    }

    int m;
    try {
      m = Integer.parseInt(args[1]);
    } catch (NumberFormatException e) {
      System.out.println("<port-number> must be an integer. Given: " + args[1] + ".");
      return;
    }
    if (m > availableNodes.size()) {
      System.out.println("Requested number of nodes is larger than available number of nodes.\n");
      return;
    }

    String zkAddress = args[2];

    int zkPort;
    try {
      zkPort = Integer.parseInt(args[3]);
    } catch (NumberFormatException e) {
      System.out.println("<port-number> must be an integer. Given: " + args[3] + ".");
      return;
    }
    if (zkPort < 1024 || 65535 < zkPort) {
      System.out.println(
          "<port-number> must be an integer between 1024 and 65535. Given: " + zkPort + ".");
      return;
    }

    if (args.length == 4) {
      ecsClient = new ECSClient(availableNodes, m, zkAddress, zkPort);
    } else {
      CacheStrategy cacheStrategy;
      try {
        cacheStrategy = CacheStrategy.valueOf(args[4]);
      } catch (IllegalArgumentException e) {
        System.out.println(
            "<cache-strategy> must be one of \"FIFO\", \"LRU\", \"LFU\" and \"Concurrent\". Given:"
                + " \""
                + args[4]
                + "\".");
        return;
      }

      int cacheSize;
      try {
        cacheSize = Integer.parseInt(args[5]);
      } catch (NumberFormatException e) {
        System.out.println("<max-cache-size> must be an integer. given: " + args[5] + ".");
        return;
      }

      ecsClient = new ECSClient(availableNodes, m, zkAddress, zkPort, cacheStrategy, cacheSize);
    }
    new ECSCLI().run();
  }

  public void run() {
    BufferedReader stdin =
        new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
    while (!stop) {
      CLIECSUtils.printPrompt();
      try {
        String cmdLine = stdin.readLine();
        handleCommand(cmdLine);
      } catch (IOException e) {
        stop = true;
        CLIECSUtils.printError("CLI does not respond - Application terminated ");
      }
    }
  }

  public void handleCommand(String cmdLine) {
    final String[] tokens = tokenize(cmdLine);
    switch (tokens[0]) {
      case "":
        break;
      case "start":
        CLIECSUtils.printRequestResult(ecsClient.start());
        break;
      case "stop":
        CLIECSUtils.printRequestResult(ecsClient.stop());
        break;
      case "shutDown":
        CLIECSUtils.printRequestResult(ecsClient.shutdown());
        break;
      case "addNode":
        CLIECSUtils.printRequestResult((ecsClient.addNode() != null));
        break;
      case "removeNode":
        CLIECSUtils.printRequestResult(ecsClient.removeNode(tokens[1]));
        break;
      case "quit":
        CLIECSUtils.printRequestResult(ecsClient.shutdown());
        quit();
        break;
      case "printMetadata":
        CLIECSUtils.printMetadata(ECSMetadata.getInstance());
        break;
      case "help":
        CLIECSUtils.printHelp();
        break;
      default:
        CLIECSUtils.printError("Unknown command: " + tokens[0]);
        CLIECSUtils.printHelp();
    }
  }

  private static String[] tokenize(String cmdLine) {
    if (cmdLine == null) {
      return new String[] {"quit"};
    }
    cmdLine = cmdLine.trim();
    String[] tokens = cmdLine.split("\\s+", -1);
    if ((tokens[0].equals("removeNode") && tokens.length == 2)
        || (!tokens[0].equals("removeNode") && tokens.length == 1)) {
      return tokens;
    } else {
      CLIECSUtils.printError("Invalid number of arguments.");
      CLIECSUtils.printHelp();
      return new String[] {""};
    }
  }

  private void quit() {
    stop = true;
    CLIECSUtils.printMessage("Application exit!");
  }
}
