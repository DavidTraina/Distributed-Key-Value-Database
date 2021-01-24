package app_kvClient;

import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class CLIUtils {
  private static final Logger logger = Logger.getRootLogger();
  private static final String PROMPT = "Client> ";

  protected static String setLevel(String levelString) {
    Level level = Level.toLevel(levelString, null);
    if (level == null) {
      return LogSetup.UNKNOWN_LEVEL;
    }
    logger.setLevel(level);
    return level.toString();
  }

  protected static void printError(String error) {
    System.out.println(PROMPT + "Error! " + error);
  }

  protected static void printMessage(String message) {
    System.out.println(PROMPT + message);
  }

  protected static void printHelp() {
    StringBuilder sb = new StringBuilder();
    sb.append(PROMPT).append("CLIENT APP HELP (Usage):\n");
    sb.append(PROMPT);
    sb.append("::::::::::::::::::::::::::::::::");
    sb.append("::::::::::::::::::::::::::::::::\n");
    sb.append(PROMPT).append("connect <host> <port>");
    sb.append("\t establishes a connection to a server\n");
    sb.append(PROMPT).append("put <key> <value>");
    sb.append("\t sends a put request to the storage server \n");
    sb.append(PROMPT).append("get <key>");
    sb.append("\t\t sends a get request to the storage server \n");
    sb.append(PROMPT).append("disconnect");
    sb.append("\t\t disconnects from the server \n");

    sb.append(PROMPT).append("logLevel");
    sb.append("\t\t changes the logLevel \n");
    sb.append(PROMPT).append("\t\t\t ");
    sb.append("ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF \n");

    sb.append(PROMPT).append("quit ");
    sb.append("\t\t\t exits the program");
    System.out.println(sb.toString());
  }

  protected static void printPossibleLogLevels() {
    System.out.println(PROMPT + "Possible log levels are:");
    System.out.println(PROMPT + "ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF");
  }

  public static void printPrompt() {
    System.out.print(PROMPT);
  }
}
