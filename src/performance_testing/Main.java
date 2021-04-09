package performance_testing;

import client.KVStoreException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.*;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class Main {
  private static final Logger logger = Logger.getRootLogger();

  public static void main(String[] args) throws IOException, KVStoreException {
    Cleaner.clean();
    try {
      new LogSetup("logs/perftest.log", Level.ERROR, false);
    } catch (IOException e) {
      System.out.println("Error! Unable to initialize logger!");
      e.printStackTrace();
      System.exit(1);
    }
    if (args.length != 3) {
      System.out.println("Please provide 3 args: <writeRatio> <numClients> <numRequests>");
      System.exit(1);
    }
    float writeRatio = Float.parseFloat(args[0]);
    int numClients = Integer.parseInt(args[1]);
    int numRequests = Integer.parseInt(args[2]);

    logger.info("Starting Perf Test");
    runPerfTest(writeRatio, numClients, numRequests);
    logger.info("Done Perf Test");
  }

  private static void runPerfTest(
      final float writeRatio, final int numClients, final int numRequests)
      throws IOException, KVStoreException {
    ExecutorService executor = Executors.newFixedThreadPool(numClients);
    List<Future<Metrics>> futureMetrics = new ArrayList<>();
    InetAddress address = null;
    try {
      address = InetAddress.getByName("localhost");
    } catch (UnknownHostException e) {
      logger.error("Invalid address");
      System.exit(1);
    }

    HashMap<String, String> kv =
        PerfUtils.loadEnron(System.getProperty("user.dir") + "/maildir", numRequests * numClients);
    //    HashMap<String, String> kv = PerfUtils.createRandom(numRequests*numClients);
    ArrayList<HashMap<String, String>> kvPerClient = PerfUtils.split(numClients, kv);

    for (int i = 0; i < numClients; i++) {
      Callable<Metrics> callable =
          new ClientWorker(i, address, 5000, writeRatio, numRequests, kvPerClient.get(i));
      Future<Metrics> future = executor.submit(callable);
      futureMetrics.add(future);
    }

    List<Metrics> results = new ArrayList<>();
    for (Future<Metrics> futureMetric : futureMetrics) {
      try {
        results.add(futureMetric.get());
      } catch (InterruptedException | ExecutionException e) {
        e.printStackTrace();
      }
    }
    executor.shutdown();
    printResults(results, writeRatio, numClients, numRequests);
  }

  private static void printResults(
      final List<Metrics> results,
      final float writeRatio,
      final int numClients,
      final int numRequests) {
    final StringBuilder sb =
        new StringBuilder("\n\n --------------------- Results--------------------------- \n");
    sb.append("\n").append("Write Ratio: ").append(writeRatio);
    sb.append("\n").append("Number of Clients: ").append(numClients);
    sb.append("\n").append("Number of Iterations: ").append(numRequests);
    sb.append("\n")
        .append("Read Latency (ms): ")
        .append(results.stream().mapToDouble(Metrics::getAverageReadLatency).average().orElse(-1));
    sb.append("\n")
        .append("Write Latency (ms): ")
        .append(results.stream().mapToDouble(Metrics::getAverageWriteLatency).average().orElse(-1));
    sb.append("\n")
        .append("Read Throughput (reads/s): ")
        .append(results.stream().mapToDouble(Metrics::getReadThroughput).average().orElse(-1));
    sb.append("\n")
        .append("Write Throughput (writes/s): ")
        .append(results.stream().mapToDouble(Metrics::getWriteThroughput).average().orElse(-1));
    sb.append("\n");
    System.out.print(sb.toString());
  }
}
