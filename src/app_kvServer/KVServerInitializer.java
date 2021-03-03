package app_kvServer;

import app_kvServer.data.SynchronizedKVManager;
import app_kvServer.data.cache.CacheStrategy;
import org.apache.log4j.Logger;

public class KVServerInitializer {

  private static final Logger logger = Logger.getLogger(KVServerInitializer.class);

  /**
   * Main entry point for the KVServer application.
   *
   * @param args expected to be equal to [<port-number>, <max-cache-size>, <cache-strategy>]
   */
  public static void main(final String[] args) {
    if (!(args.length == 3 || args.length == 6)) {
      KVServerInitializer.exitWithErrorMessage(
          "Exactly 3 or 6 arguments required. " + args.length + " provided.");
      return;
    }

    // Validate <port-number>
    int port = -1;
    try {
      port = Integer.parseInt(args[0]);
    } catch (NumberFormatException e) {
      KVServerInitializer.exitWithErrorMessage(
          "<port-number> must be an integer. Given: " + args[0] + ".");
    }
    if (port < 1024 || 65535 < port) {
      KVServerInitializer.exitWithErrorMessage(
          "<port-number> must be an integer between 1024 and 65535. Given: " + port + ".");
    }

    // Validate <cache-size>
    int cacheSize = -1;
    try {
      cacheSize = Integer.parseInt(args[1]);
    } catch (NumberFormatException e) {
      KVServerInitializer.exitWithErrorMessage(
          "<max-cache-size> must be an integer. given: " + args[1] + ".");
    }

    // Validate <cache-strategy>
    CacheStrategy cacheStrategy = null;
    try {
      cacheStrategy = CacheStrategy.valueOf(args[2]);
    } catch (IllegalArgumentException e) {
      KVServerInitializer.exitWithErrorMessage(
          "<cache-strategy> must be one of \"FIFO\", \"LRU\", \"LFU\" and \"Concurrent\". Given: \""
              + args[2]
              + "\".");
    }

    // ECS initialized the server
    if (args.length == 6) {
      String zookeeperIP = args[3];
      int zookeeperPort = Integer.parseInt(args[4]);
      String nodeName = args[5];

      startServerViaECS(port, cacheSize, cacheStrategy, zookeeperIP, zookeeperPort, nodeName);
    } else {
      startServer(port, cacheSize, cacheStrategy);
    }
  }

  private static void exitWithErrorMessage(String errorMessage) {
    System.out.println("Error! Invalid arguments: " + errorMessage + "\n");
    System.out.println(
        "Usage: Server <port-number> <max-cache-size> <cache-strategy> [<zookeeper-ip>"
            + " <zookeeper-port> <node-name>]");
    System.out.format(
        "%-32s%32s%n", "\t<port-number>", "The port number for the Server to listen on.");
    System.out.format(
        "%-32s%32s%n",
        "\t<max-cache-size>",
        "The maximum number of key-value pairs that can be cached in memory. Non positive disables"
            + " caching.");
    System.out.format(
        "%-32s%32s%n",
        "\t<port-number>",
        "The type of cache to use. Options are: \"FIFO\", \"LRU\", \"LFU\" and \"Concurrent\".");
    throw new IllegalArgumentException(errorMessage);
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
    SynchronizedKVManager.initialize(cacheSize, cacheStrategy, "localhost:" + port);
    logger.info("Starting KVServer from Main");
    new Thread(new KVServer(port), "KVServer@" + port).start();
  }

  private static void startServerViaECS(
      final int port,
      final int cacheSize,
      final CacheStrategy cacheStrategy,
      final String zookeeperIP,
      final int zookeeperPort,
      final String nodeName) {
    SynchronizedKVManager.initialize(cacheSize, cacheStrategy, nodeName);
    logger.info("Starting KVServer from Main");
    new Thread(new KVServer(port, zookeeperIP, zookeeperPort, nodeName), "KVServer@" + port)
        .start();
  }
}
