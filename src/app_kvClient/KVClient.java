package app_kvClient;

import client.KVStore;
import client.KVStoreException;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import shared.messages.KVMessage;

public class KVClient {
  private static final Logger logger = Logger.getRootLogger();
  private boolean stop = false;
  private KVStore store = null;

  /**
   * Main entry point for the application.
   *
   * @param args contains the port number at args[0].
   */
  public static void main(String[] args) {
    try {
      new LogSetup("logs/client.log", Level.OFF);
    } catch (IOException e) {
      System.out.println("Error! Unable to initialize logger!");
      e.printStackTrace();
      System.exit(1);
    }
    KVClient app = new KVClient();
    app.run();
  }

  public void run() {
    BufferedReader stdin =
        new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
    while (!stop) {
      CLIUtils.printPrompt();
      try {
        String cmdLine = stdin.readLine();
        handleCommand(cmdLine);
      } catch (IOException e) {
        stop = true;
        logger.error("CLI does not respond - Application terminated ");
      }
    }
  }

  public void handleCommand(String cmdLine) {
    final String[] tokens = tokenize(cmdLine);

    switch (tokens[0]) {
      case "":
        break;
      case "quit":
        handleQuitCommand();
        break;
      case "connect":
        handleConnectCommand(tokens);
        break;
      case "put":
        handlePutCommand(tokens);
        break;
      case "get":
        handleGetCommand(tokens);
        break;
      case "disconnect":
        handleDisconnectCommand();
        break;
      case "logLevel":
        handleLogLevelCommand(tokens);
        break;
      case "help":
        CLIUtils.printHelp();
        break;
      default:
        CLIUtils.printError("Unknown command: " + tokens[0]);
        CLIUtils.printHelp();
    }
  }

  private static String[] tokenize(String cmdLine) {
    if (cmdLine == null) {
      System.out.println();
      return new String[] {"quit"};
    }
    cmdLine = cmdLine.trim();
    final String cmd = cmdLine.split("\\s+", 2)[0];
    return cmdLine.split("\\s+", cmd.equals("put") ? 3 : -1);
  }

  private void handleQuitCommand() {
    stop = true;
    if (store != null) {
      try {
        store.disconnect();
      } catch (KVStoreException e) {
        logger.error("Could not disconnect", e);
      }
    }
    CLIUtils.printMessage("Application exit!");
  }

  private void handleConnectCommand(String[] tokens) {
    if (tokens.length != 3) {
      CLIUtils.printError("Invalid number of parameters!");
      return;
    }

    InetAddress serverAddress;
    try {
      serverAddress = InetAddress.getByName(tokens[1]);
    } catch (UnknownHostException e) {
      CLIUtils.printError("Unable to resolve hostname or IP address: " + tokens[1]);
      logger.info("Invalid Hostname or IP Address given: " + tokens[1], e);
      return;
    }

    int serverPort;
    try {
      serverPort = Integer.parseInt(tokens[2]);
    } catch (NumberFormatException nfe) {
      CLIUtils.printError("Unable to parse port number! Port number must be a positive integer.");
      logger.info("Invalid port number given: " + tokens[2], nfe);
      return;
    }
    if (serverPort < 1024 || 65535 < serverPort) {
      CLIUtils.printError(
          "Port number: " + serverPort + " is outside of legal range from 1024 to 65535.");
      logger.info(
          "Given port number: " + serverPort + " is outside of legal range from 1024 to 65535.");
      return;
    }
    try {
      CLIUtils.printMessage("Attempting to connect to server...");
      tryConnectingToServer(serverAddress, serverPort);
      CLIUtils.printMessage(
          "Connection established to " + serverAddress.getCanonicalHostName() + ":" + serverPort);
      logger.info("Client connected to " + serverAddress.getCanonicalHostName() + ":" + serverPort);
    } catch (KVStoreException e) {
      CLIUtils.printError("FAILED<Could not connect to server>");
      logger.error("Failed to establish a new connection", e);
    }
  }

  private void handlePutCommand(String[] tokens) {
    if (tokens.length != 2 && tokens.length != 3) {
      CLIUtils.printError("Incorrect number of args!");
      return;
    }
    String key = tokens[1];
    String value = tokens.length >= 3 ? tokens[2] : null;
    try {
      KVMessage putReply = store.put(key, value);
      handlePutReply(putReply);
    } catch (KVStoreException e) {
      CLIUtils.printError("Error communicating with server");
      logger.error(e);
    }
  }

  private void handleGetCommand(String[] tokens) {
    if (tokens.length != 2) {
      CLIUtils.printError("Incorrect number of args!");
      return;
    }
    String key = tokens[1];
    try {
      KVMessage getReply = store.get(key);
      handleGetReply(getReply);
    } catch (KVStoreException e) {
      CLIUtils.printError("Error communicating with server");
      logger.error(e);
    }
  }

  private void handleDisconnectCommand() {
    if (store == null) {
      CLIUtils.printMessage("You are not connected to the server!");
      return;
    }
    try {
      store.disconnect();
    } catch (KVStoreException e) {
      logger.error(e);
    }
    store = null;
    CLIUtils.printMessage("Disconnected from server.");
  }

  private void handleLogLevelCommand(String[] tokens) {
    if (tokens.length != 2) {
      CLIUtils.printError("Invalid number of parameters!");
      return;
    }
    String level = CLIUtils.setLevel(tokens[1]);
    changeLogLevel(level);
  }

  private void handlePutReply(KVMessage putReply) {
    switch (putReply.getStatus()) {
      case PUT_ERROR:
        CLIUtils.printError("PUT_ERROR<" + putReply.getKey() + "," + putReply.getValue() + ">");
        break;
      case PUT_SUCCESS:
        CLIUtils.printMessage("PUT_SUCCESS<" + putReply.getKey() + "," + putReply.getValue() + ">");
        break;
      case PUT_UPDATE:
        CLIUtils.printMessage("PUT_UPDATE<" + putReply.getKey() + "," + putReply.getValue() + ">");
        break;
      case DELETE_ERROR:
        CLIUtils.printError("DELETE_ERROR<" + putReply.getKey() + ">");
        break;
      case DELETE_SUCCESS:
        CLIUtils.printMessage("DELETE_SUCCESS<" + putReply.getKey() + ">");
        break;
      case FAILED:
        CLIUtils.printMessage("FAILED<" + putReply.getKey() + ">");
        break;
      default:
        CLIUtils.printError("FAILED<Reply doesn't match request>");
    }
  }

  private void handleGetReply(KVMessage getReply) {
    switch (getReply.getStatus()) {
      case GET_ERROR:
        CLIUtils.printError("GET_ERROR<" + getReply.getKey() + ">");
        break;
      case GET_SUCCESS:
        CLIUtils.printMessage("GET_SUCCESS<" + getReply.getKey() + "," + getReply.getValue() + ">");
        break;
      case FAILED:
        CLIUtils.printMessage("FAILED<" + getReply.getKey() + ">");
        break;
      default:
        CLIUtils.printError("FAILED<Reply doesn't match request>");
    }
  }

  private void changeLogLevel(String level) {
    if (level.equals(LogSetup.UNKNOWN_LEVEL)) {
      CLIUtils.printError("No valid log level!");
      CLIUtils.printPossibleLogLevels();
    } else {
      CLIUtils.printMessage("Log level changed to level " + level);
    }
  }

  /**
   * Creates a new connection to the given address at teh given port.
   *
   * @param serverAddress The address of the server.
   * @param serverPort the port number, from 1024-65535, to connect to.
   * @throws KVStoreException If a connection is unable to be established.
   */
  private void tryConnectingToServer(InetAddress serverAddress, int serverPort)
      throws KVStoreException {
    store = new KVStore(serverAddress, serverPort);
    store.connect();
  }
}
