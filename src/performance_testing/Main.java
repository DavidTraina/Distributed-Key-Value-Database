package performance_testing;

import info.debatty.java.datasets.enron.Dataset;
import info.debatty.java.datasets.enron.Email;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Spliterator;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class Main {
  private static final Logger logger = Logger.getRootLogger();

  public static void main(String[] args) {
    try {
      new LogSetup("logs/perftest.log", Level.ERROR, false);
    } catch (IOException e) {
      System.out.println("Error! Unable to initialize logger!");
      e.printStackTrace();
      System.exit(1);
    }

    float writeRatio = 0.5f;
    int numClients = 10;
    logger.info("Starting Perf Test");
    runPerfTest(writeRatio, numClients);
    logger.info("Done Perf Test");
  }

  private static void runPerfTest(final float writeRatio, final int numClients) {
    Dataset enronDataset = new Dataset(System.getProperty("user.dir") + "/maildir");
    Spliterator<Email> emailsSpliterator = enronDataset.spliterator();

    ExecutorService executor = Executors.newFixedThreadPool(numClients);

    List<Future<Metrics>> futureMetrics;
    try {
      final InetAddress address = InetAddress.getByName("localhost");
      List<Callable<Metrics>> workers =
          IntStream.range(0, numClients)
              .mapToObj(i -> new EnronClientWorker(address, 50000, writeRatio, emailsSpliterator))
              .collect(Collectors.toList());
      futureMetrics = new ArrayList<>(executor.invokeAll(workers));
    } catch (UnknownHostException e) {
      logger.error("Invalid address");
      throw new RuntimeException(e);
    } catch (InterruptedException e) {
      e.printStackTrace();
      throw new RuntimeException(e);
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
    Integer numRequests =
        results.stream()
            .map(mets -> (int) (mets.getNumGets() + mets.getNumPuts()))
            .reduce(0, Integer::sum);
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
