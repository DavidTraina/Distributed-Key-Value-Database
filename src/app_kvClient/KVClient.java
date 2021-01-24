package app_kvClient;

import client.KVCommInterface;
import client.KVStore;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.UnknownHostException;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import shared.exceptions.NotConnectedException;
import shared.messages.KVMessage;

public class KVClient implements IKVClient {
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
      KVClient app = new KVClient();
      app.run();
    } catch (IOException e) {
      System.out.println("Error! Unable to initialize logger!");
      e.printStackTrace();
      System.exit(1);
    }
  }

  public void run() {
    while (!stop) {
      BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in));
      CLIUtils.printPrompt();

      try {
        String cmdLine = stdin.readLine();
        this.handleCommand(cmdLine);
      } catch (IOException e) {
        stop = true;
        logger.error("CLI does not respond - Application terminated ");
      }
    }
  }

  public void handleCommand(String cmdLine) {
    String[] tokens = cmdLine.split("\\s+");

    switch (tokens[0]) {
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
        CLIUtils.printError("Unknown command");
        CLIUtils.printHelp();
    }
  }

  private void handleQuitCommand() {
    stop = true;
    if (store != null) store.disconnect();
    CLIUtils.printMessage("Application exit!");
  }

  private void handleConnectCommand(String[] tokens) {
    if (tokens.length == 3) {
      try {
        String serverAddress = tokens[1];
        int serverPort = Integer.parseInt(tokens[2]);
        tryConnectingToServer(serverAddress, serverPort);
      } catch (NumberFormatException nfe) {
        CLIUtils.printError("No valid address. Port must be a number!");
        logger.info("Unable to parse argument <port>", nfe);
      }
    } else {
      CLIUtils.printError("Invalid number of parameters!");
    }
  }

  private void handlePutCommand(String[] tokens) {
    if (tokens.length >= 2) {
      String key = tokens[1];
      String value = tokens.length >= 3 ? tokens[2] : null;
      // TODO: Handle shared.exceptions properly for put
      try {
        KVMessage putReply = tryPut(key, value);
        handlePutReply(putReply);
      } catch (NotConnectedException nce) {
        CLIUtils.printError("Not connected to a server");
      } catch (Exception e) {
        e.printStackTrace();
      }
    } else {
      CLIUtils.printError("Incorrect number of args!");
    }
  }

  private void handleGetCommand(String[] tokens) {
    if (tokens.length >= 2) {
      String key = tokens[1];
      // TODO: Handle shared.exceptions properly for get
      try {
        KVMessage getReply = tryGet(key);
        handleGetReply(getReply);
      } catch (NotConnectedException nce) {
        CLIUtils.printError("Not connected to a server");
      } catch (Exception e) {
        e.printStackTrace();
      }
    } else {
      CLIUtils.printError("Incorrect number of args, expected 1 - get <key>");
    }
  }

  private void handleDisconnectCommand() {
    if (store != null) store.disconnect();
    store = null;
    CLIUtils.printMessage("Disconnected from server");
  }

  private void handleLogLevelCommand(String[] tokens) {
    if (tokens.length == 2) {
      String level = CLIUtils.setLevel(tokens[1]);
      changeLogLevel(level);
    } else {
      CLIUtils.printError("Invalid number of parameters!");
    }
  }

  private void handlePutReply(KVMessage putReply) {
    switch (putReply.getStatus()) {
      case PUT_ERROR:
        CLIUtils.printError("Put Error");
        break;
      case PUT_SUCCESS:
        CLIUtils.printMessage(
            "Successfully added new key "
                + putReply.getKey()
                + "with value "
                + putReply.getValue());
        break;
      case PUT_UPDATE:
        CLIUtils.printMessage(
            "Successfully updated key " + putReply.getKey() + "with value " + putReply.getValue());
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
        CLIUtils.printError("Put Error");
        break;
      case GET_SUCCESS:
        CLIUtils.printMessage(
            "GET Successful, result ->  " + getReply.getKey() + " Value " + getReply.getValue());
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

  private KVMessage tryGet(String key) throws Exception {
    if (store != null) {
      return store.get(key);
    } else {
      throw new NotConnectedException("No valid KVStore object");
    }
  }

  private KVMessage tryPut(String key, String value) throws Exception {
    if (store != null) {
      return store.put(key, value);
    } else {
      throw new NotConnectedException("No valid KVStore object");
    }
  }

  private void tryConnectingToServer(String serverAddress, int serverPort) {
    // TODO: Handle shared.exceptions properly for newConnection
    try {
      newConnection(serverAddress, serverPort);
    } catch (UnknownHostException e) {
      CLIUtils.printError("Unknown Host!");
      logger.info("Unknown Host!", e);
    } catch (IOException e) {
      CLIUtils.printError("Could not establish connection!");
      logger.warn("Could not establish connection!", e);
    } catch (Exception e) {
      CLIUtils.printError("Could not establish connection!");
      logger.warn("Could not establish connection!", e);
    }
  }

  @Override
  public void newConnection(String hostname, int port) throws Exception {
    store = new KVStore(hostname, port);
    store.connect();
  }

  @Override
  public KVCommInterface getStore() {
    return store;
  }
}
