package app_kvServer;

import app_kvServer.data.SynchronizedKVManager;
import app_kvServer.data.cache.CacheStrategy;
import java.io.IOException;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class Main {

  private static final Logger logger = Logger.getRootLogger();

  /**
   * Main entry point for the KVServer application.
   *
   * @param args expected to be equal to [<port-number>, <max-cache-size>, <cache-strategy>]
   */
  public static void main(final String[] args) {
    if (args.length != 3) {
      Main.exitWithErrorMessage("Exactly 3 arguments required. " + args.length + " provided.");
    }

    // Validate <port-number>
    int port = -1;
    try {
      port = Integer.parseInt(args[0]);
    } catch (NumberFormatException e) {
      Main.exitWithErrorMessage("<port-number> must be an integer. Given: " + args[0] + ".");
    }
    if (port < 1024 || 65535 < port) {
      Main.exitWithErrorMessage(
          "<port-number> must be an integer between 1024 and 65535. Given: " + port + ".");
    }

    // Validate <cache-size>
    int cacheSize = -1;
    try {
      cacheSize = Integer.parseInt(args[1]);
    } catch (NumberFormatException e) {
      Main.exitWithErrorMessage("<max-cache-size> must be an integer. given: " + args[1] + ".");
    }
    if (cacheSize < 1) {
      Main.exitWithErrorMessage(
          "<max-cache-size> must be a positive integer. Given: " + cacheSize + ".");
    }

    // Validate <cache-strategy>
    CacheStrategy cacheStrategy = null;
    try {
      cacheStrategy = CacheStrategy.valueOf(args[2]);
    } catch (IllegalArgumentException e) {
      Main.exitWithErrorMessage(
          "<cache-strategy> must be one of \"FIFO\", \"LRU\", \"LFU\" and \"Concurrent\". Given: \""
              + args[2]
              + "\".");
    }

    try {
      new LogSetup("logs/server.log", Level.ALL);
    } catch (IOException e) {
      System.out.println("Error! Unable to initialize logger!");
      e.printStackTrace();
      System.exit(1);
    }

    startServer(port, cacheSize, cacheStrategy);
  }

  private static void exitWithErrorMessage(String errorMessage) {
    System.out.println("Error! Invalid arguments: " + errorMessage + "\n");
    System.out.println("Usage: Server <port-number> <max-cache-size> <cache-strategy>");
    System.out.format(
        "%-32s%32s%n", "\t<port-number>", "The port number for the Server to listen on.");
    System.out.format(
        "%-32s%32s%n",
        "\t<max-cache-size>", "The maximum number of key-value pairs that can be cached in memory");
    System.out.format(
        "%-32s%32s%n",
        "\t<port-number>",
        "The type of cache to use. Options are: \"FIFO\", \"LRU\", \"LFU\" and \"Concurrent\".");
    System.exit(1);
  }

  /**
   * Start KV Server at given port
   *
   * @param port given port for storage server to operate
   * @param cacheSize specifies how many key-value pairs the server is allowed to keep in-memory
   * @param cacheStrategy specifies the cache replacement strategy in case the cache is full and
   *     there is a GET- or PUT-request on a key that is currently not contained in the cache.
   *     Options are "FIFO", "LRU", "LFU" and "Concurrent".
   */
  private static void startServer(
      final int port, final int cacheSize, final CacheStrategy cacheStrategy) {
    SynchronizedKVManager.initialize(cacheSize, cacheStrategy);
    logger.info("Starting KVServer from Main");
    new Thread(new KVServer(port)).start();
  }
}
