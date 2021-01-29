package app_kvClient;

import client.KVStore;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import shared.exceptions.ConnectionLostException;
import shared.exceptions.NotConnectedException;
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
    if (store != null) store.disconnect();
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
    } catch (IOException e) {
      CLIUtils.printError("Unknown error establishing connection!");
      logger.error("Error establishing connection", e);
    }
  }

  private void handlePutCommand(String[] tokens) {
    if (tokens.length != 2 && tokens.length != 3) {
      CLIUtils.printError("Incorrect number of args!");
      return;
    }
    String key = tokens[1];
    String value = tokens.length == 3 ? tokens[2] : null;
    // TODO: Handle shared.exceptions properly for put
    try {
      KVMessage putReply = tryPut(key, value);
      handlePutReply(putReply);
    } catch (NotConnectedException nce) {
      CLIUtils.printError("Not connected to a server, please connect.");
      logger.info("User attempted PUT before connecting.");
    } catch (IOException e) {
      CLIUtils.printError("Unknown error communicating with server.");
      logger.error("Unknown error communicating with server on PUT request.", e);
    } catch (ConnectionLostException e) {
      CLIUtils.printError("Connection to server lost.");
      logger.error("Connection to server was lost during PUT request.", e);
    }
  }

  private void handleGetCommand(String[] tokens) {
    if (tokens.length != 2) {
      CLIUtils.printError("Incorrect number of args!");
      return;
    }
    String key = tokens[1];
    try {
      KVMessage getReply = tryGet(key);
      handleGetReply(getReply);
    } catch (NotConnectedException nce) {
      CLIUtils.printError("Not connected to a server, please connect.");
    } catch (IOException e) {
      CLIUtils.printError("Error communicating with server.");
      logger.error(e);
    } catch (ConnectionLostException e) {
      CLIUtils.printError("Connection to server lost.");
      logger.error(e);
    }
  }

  private void handleDisconnectCommand() {
    if (store == null) {
      CLIUtils.printMessage("You are not connected to the server!");
      return;
    }
    store.disconnect();
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
        CLIUtils.printError("PUT Error");
        break;
      case PUT_SUCCESS:
        CLIUtils.printMessage(
            "Successfully added new key "
                + putReply.getKey()
                + " with value "
                + putReply.getValue());
        break;
      case PUT_UPDATE:
        CLIUtils.printMessage(
            "Successfully updated key " + putReply.getKey() + " with value " + putReply.getValue());
        break;
      case DELETE_ERROR:
        CLIUtils.printMessage("Delete Error");
        break;
      case DELETE_SUCCESS:
        CLIUtils.printMessage("Successfully deleted Key " + putReply.getKey());
        break;
    }
  }

  private void handleGetReply(KVMessage getReply) {
    switch (getReply.getStatus()) {
      case GET_ERROR:
        CLIUtils.printError("GET ERROR: key <" + getReply.getKey() + "> not present.");
        break;
      case GET_SUCCESS:
        CLIUtils.printMessage(
            "<key: " + getReply.getKey() + " value: " + getReply.getValue() + ">");
        break;
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

  private KVMessage tryGet(String key)
      throws IOException, NotConnectedException, ConnectionLostException {
    if (store == null) {
      throw new NotConnectedException("No valid KVStore object");
    }
    return store.get(key);
  }

  private KVMessage tryPut(String key, String value)
      throws IOException, NotConnectedException, ConnectionLostException {
    if (store == null) {
      throw new NotConnectedException("No valid KVStore object");
    }
    return store.put(key, value);
  }

  /**
   * Creates a new connection to the given address at teh given port.
   *
   * @param serverAddress The address of the server.
   * @param serverPort the port number, from 1024-65535, to connect to.
   * @throws IOException If a connection is unable to be established.
   */
  private void tryConnectingToServer(InetAddress serverAddress, int serverPort) throws IOException {
    store = new KVStore(serverAddress, serverPort);
    store.connect();
  }
}
