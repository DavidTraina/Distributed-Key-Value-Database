package app_kvClient;

import app_kvECS.CLIECSUtils;
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
import shared.communication.messages.KVMessage;

public class KVClient {
  private static final Logger logger = Logger.getLogger(KVClient.class);
  private boolean stop = false;
  private KVStore store = null;

  /**
   * Main entry point for the application.
   *
   * @param args contains the port number at args[0].
   */
  public static void main(String[] args) {
    try {
      new LogSetup("logs/client.log", Level.DEBUG, true);
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
      CLIClientUtils.printPrompt();
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
    final String[] tokens = CLIECSUtils.tokenize(cmdLine);

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
        CLIClientUtils.printHelp();
        break;
      default:
        CLIClientUtils.printError("Unknown command: " + tokens[0]);
        CLIClientUtils.printHelp();
    }
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
    CLIClientUtils.printMessage("Application exit!");
  }

  private void handleConnectCommand(String[] tokens) {
    if (tokens.length != 3) {
      CLIClientUtils.printError("Invalid number of parameters!");
      return;
    }

    InetAddress serverAddress;
    try {
      serverAddress = InetAddress.getByName(tokens[1]);
    } catch (UnknownHostException e) {
      CLIClientUtils.printError("Unable to resolve hostname or IP address: " + tokens[1]);
      logger.info("Invalid Hostname or IP Address given: " + tokens[1], e);
      return;
    }

    int serverPort;
    try {
      serverPort = Integer.parseInt(tokens[2]);
    } catch (NumberFormatException nfe) {
      CLIClientUtils.printError(
          "Unable to parse port number! Port number must be a positive integer.");
      logger.info("Invalid port number given: " + tokens[2], nfe);
      return;
    }
    if (serverPort < 1024 || 65535 < serverPort) {
      CLIClientUtils.printError(
          "Port number: " + serverPort + " is outside of legal range from 1024 to 65535.");
      logger.info(
          "Given port number: " + serverPort + " is outside of legal range from 1024 to 65535.");
      return;
    }
    try {
      CLIClientUtils.printMessage("Attempting to connect to server...");
      tryConnectingToServer(serverAddress, serverPort);
      CLIClientUtils.printMessage(
          "Connection established to " + serverAddress.getCanonicalHostName() + ":" + serverPort);
      logger.info("Client connected to " + serverAddress.getCanonicalHostName() + ":" + serverPort);
    } catch (KVStoreException e) {
      CLIClientUtils.printError("FAILED<Could not connect to server>");
      logger.error("Failed to establish a new connection", e);
    }
  }

  private void handlePutCommand(String[] tokens) {
    if (tokens.length != 2 && tokens.length != 3) {
      CLIClientUtils.printError("Incorrect number of args!");
      return;
    }
    if (store == null) {
      CLIClientUtils.printError("Please connect to a server first.");
      return;
    }
    String key = tokens[1];
    String value = tokens.length >= 3 ? tokens[2] : null;
    try {
      KVMessage putReply = store.put(key, value);
      handlePutReply(putReply);
    } catch (KVStoreException e) {
      CLIClientUtils.printError("Error communicating with server");
      logger.error("Error during PUT request: ", e);
    }
  }

  private void handleGetCommand(String[] tokens) {
    if (tokens.length != 2) {
      CLIClientUtils.printError("Incorrect number of args!");
      return;
    }
    if (store == null) {
      CLIClientUtils.printError("Please connect to a server first.");
      return;
    }
    String key = tokens[1];
    try {
      KVMessage getReply = store.get(key);
      handleGetReply(getReply);
    } catch (KVStoreException e) {
      CLIClientUtils.printError("Error communicating with server");
      logger.error("Error during GET request: ", e);
    }
  }

  private void handleDisconnectCommand() {
    if (store == null) {
      CLIClientUtils.printMessage("You are not connected to the server!");
      return;
    }
    try {
      store.disconnect();
    } catch (KVStoreException e) {
      logger.error("Error during disconnect: ", e);
    }
    store = null;
    CLIClientUtils.printMessage("Disconnected from server.");
  }

  private void handleLogLevelCommand(String[] tokens) {
    if (tokens.length != 2) {
      CLIClientUtils.printError("Invalid number of parameters!");
      return;
    }
    String level = CLIClientUtils.setLevel(tokens[1]);
    changeLogLevel(level);
  }

  private void handlePutReply(KVMessage putReply) {
    switch (putReply.getStatus()) {
      case PUT_ERROR:
      case PUT_SUCCESS:
      case PUT_UPDATE:
        CLIClientUtils.printError(
            putReply.getStatus() + "<" + putReply.getKey() + "," + putReply.getValue() + ">");
        break;
      case DELETE_ERROR:
      case DELETE_SUCCESS:
        CLIClientUtils.printMessage(putReply.getStatus() + "<" + putReply.getKey() + ">");
        break;
      case FAILED:
      case SERVER_STOPPED:
      case SERVER_WRITE_LOCK:
        CLIClientUtils.printMessage(putReply.getStatus() + "<" + putReply.getErrorMessage() + ">");
        break;
      default:
        CLIClientUtils.printError("FAILED<Reply doesn't match request>");
    }
  }

  private void handleGetReply(KVMessage getReply) {
    switch (getReply.getStatus()) {
      case GET_ERROR:
        CLIClientUtils.printError(getReply.getStatus() + "<" + getReply.getKey() + ">");
        break;
      case GET_SUCCESS:
        CLIClientUtils.printMessage(
            getReply.getStatus() + "<" + getReply.getKey() + "," + getReply.getValue() + ">");
        break;
      case FAILED:
      case SERVER_STOPPED:
      case SERVER_WRITE_LOCK:
        CLIClientUtils.printMessage(getReply.getStatus() + "<" + getReply.getErrorMessage() + ">");
        break;
      default:
        CLIClientUtils.printError("FAILED<Reply doesn't match request>");
    }
  }

  private void changeLogLevel(String level) {
    if (level.equals(LogSetup.UNKNOWN_LEVEL)) {
      CLIClientUtils.printError("No valid log level!");
      CLIClientUtils.printPossibleLogLevels();
    } else {
      CLIClientUtils.printMessage("Log level changed to level " + level);
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
