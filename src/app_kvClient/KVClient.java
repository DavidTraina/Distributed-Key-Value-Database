package app_kvClient;

import static shared.communication.messages.KVMessage.StatusType.NOTIFY;

import app_kvECS.CLIECSUtils;
import client.KVStore;
import client.KVStoreException;
import ecs.ECSMetadata;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import shared.communication.messages.KVMessage;
import shared.communication.security.property_stores.ClientPropertyStore;

public class KVClient {
  private static final Logger logger = Logger.getLogger(KVClient.class);
  private boolean stop = false;
  private KVStore store = null;

  /** Main entry point for the client. */
  public static void main(String[] args) {
    try {
      new LogSetup("logs/client.log", Level.ALL, false);
    } catch (IOException e) {
      System.out.println("Error! Unable to initialize logger!");
      e.printStackTrace();
      System.exit(1);
    }
    ClientPropertyStore.getInstance().setSenderID("client");
    ECSMetadata.initialize(new ArrayList<>());
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
      case "subscribe":
      case "unsubscribe":
        handleSubscriptionCommand(tokens);
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
        CLIClientUtils.printMessage("Unknown command: " + tokens[0]);
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
      CLIClientUtils.printMessage("Invalid number of parameters!");
      return;
    }

    InetAddress serverAddress;
    try {
      serverAddress = InetAddress.getByName(tokens[1]);
    } catch (UnknownHostException e) {
      CLIClientUtils.printMessage("Unable to resolve hostname or IP address: " + tokens[1]);
      logger.info("Invalid Hostname or IP Address given: " + tokens[1], e);
      return;
    }

    int serverPort;
    try {
      serverPort = Integer.parseInt(tokens[2]);
    } catch (NumberFormatException nfe) {
      CLIClientUtils.printMessage(
          "Unable to parse port number! Port number must be a positive integer.");
      logger.info("Invalid port number given: " + tokens[2], nfe);
      return;
    }
    if (serverPort < 1024 || 65535 < serverPort) {
      CLIClientUtils.printMessage(
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
      CLIClientUtils.printMessage("FAILED<Could not connect to server>");
      logger.error("Failed to establish a new connection", e);
    }
  }

  private void handlePutCommand(String[] tokens) {
    if (tokens.length != 2 && tokens.length != 3) {
      CLIClientUtils.printMessage("Incorrect number of args!");
      return;
    }
    if (store == null) {
      CLIClientUtils.printMessage("Please connect to a server first.");
      return;
    }
    String key = tokens[1];
    String value = tokens.length >= 3 ? tokens[2] : null;
    try {
      KVMessage putReply = store.put(key, value);
      handlePutReply(putReply);
    } catch (KVStoreException e) {
      CLIClientUtils.printMessage("Error on server, error: " + e.getMessage());
      logger.error("Error during PUT request: ", e);
    }
  }

  private void handleGetCommand(String[] tokens) {
    if (tokens.length != 2) {
      CLIClientUtils.printMessage("Incorrect number of args!");
      return;
    }
    if (store == null) {
      CLIClientUtils.printMessage("Please connect to a server first.");
      return;
    }
    String key = tokens[1];
    try {
      KVMessage getReply = store.get(key);
      handleGetReply(getReply);
    } catch (KVStoreException e) {
      CLIClientUtils.printMessage("Error on server, error: " + e.getMessage());
      logger.error("Error during GET request: ", e);
    }
  }

  private void handleSubscriptionCommand(final String[] tokens) {
    if (tokens.length != 2) {
      CLIClientUtils.printMessage("Incorrect number of args!");
      return;
    }
    if (store == null) {
      CLIClientUtils.printMessage("Please connect to a server first.");
      return;
    }
    String key = tokens[1];
    try {

      KVMessage subscriptionReply =
          tokens[0].equals("subscribe") ? store.subscribe(key) : store.unsubscribe(key);
      handleSubscriptionReply(subscriptionReply);
    } catch (KVStoreException e) {
      CLIClientUtils.printMessage("Error communicating with server");
      logger.error("Error during SUBSCRIBE request: ", e);
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
      CLIClientUtils.printMessage("Invalid number of parameters!");
      return;
    }
    String level = CLIClientUtils.setLevel(tokens[1]);
    changeLogLevel(level);
  }

  private void handlePutReply(KVMessage putReply) {
    String info;
    switch (putReply.getStatus()) {
      case PUT_ERROR:
      case PUT_SUCCESS:
      case PUT_UPDATE:
        info = putReply.getKey() + ", " + putReply.getValue();
        break;
      case DELETE_ERROR:
      case DELETE_SUCCESS:
        info = putReply.getKey();
        break;
      case FAILED:
        info = "Request failed";
        break;
      case AUTH_FAILED:
        info = "Request failed due to authentication";
        break;
      case SERVER_STOPPED:
        info = "Server is stopped and cannot process requests";
        break;
      case SERVER_WRITE_LOCK:
        info = "Server is write-locked and cannot process process write requests";
        break;
      default:
        info = "Error: Unexpected response type";
        break;
    }
    CLIClientUtils.printMessage(putReply.getStatus() + "<" + info + ">");
  }

  private void handleGetReply(KVMessage getReply) {
    String info;
    switch (getReply.getStatus()) {
      case GET_SUCCESS:
        info = getReply.getKey() + ", " + getReply.getValue();
        break;
      case GET_ERROR:
        info = getReply.getKey();
        break;
      case FAILED:
        info = "Request failed";
        break;
      case AUTH_FAILED:
        info = "Request failed due to authentication";
        break;
      case SERVER_STOPPED:
        info = "Server is stopped and cannot process requests";
        break;
      default:
        info = "Error: Unexpected response type";
        break;
    }
    CLIClientUtils.printMessage(getReply.getStatus() + "<" + info + ">");
  }

  private void handleSubscriptionReply(KVMessage subscriptionReply) {
    String info;
    switch (subscriptionReply.getStatus()) {
      case SUBSCRIBE_SUCCESS:
      case SUBSCRIBE_ERROR:
      case UNSUBSCRIBE_SUCCESS:
      case UNSUBSCRIBE_ERROR:
        info = subscriptionReply.getKey();
        break;
      case FAILED:
        info = "Request failed";
        break;
      case SERVER_STOPPED:
        info = "Server is stopped and cannot process requests";
        break;
      default:
        info = "Error: Unexpected response type";
        break;
    }
    CLIClientUtils.printMessage(subscriptionReply.getStatus() + "<" + info + ">");
  }

  private void changeLogLevel(String level) {
    if (level.equals(LogSetup.UNKNOWN_LEVEL)) {
      CLIClientUtils.printMessage("No valid log level!");
      CLIClientUtils.printPossibleLogLevels();
    } else {
      CLIClientUtils.printMessage("Log level changed to level " + level);
    }
  }

  public static void receiveSubscriptionNotification(KVMessage notification) {
    assert (notification.getStatus() == NOTIFY);
    System.out.println();
    CLIClientUtils.printMessage(
        notification.getStatus()
            + "<"
            + notification.getKey()
            + ", "
            + notification.getValue()
            + ">");
    CLIClientUtils.printMessage("");
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
