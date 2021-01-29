package performance_testing;

import java.util.concurrent.TimeUnit;

public class Metrics {
  private long TotalGetLatency = 0;
  private long TotalPutLatency = 0;
  private double numPuts = 0;
  private double numGets = 0;

  protected void updatePutLatency(long latency) {
    this.TotalPutLatency += latency;
    this.numPuts += 1;
  }

  protected void updateGetLatency(long latency) {
    this.TotalGetLatency += latency;
    this.numGets += 1;
  }

  protected double getAverageWriteLatency() {
    return TimeUnit.MILLISECONDS.convert(TotalPutLatency, TimeUnit.NANOSECONDS) / numPuts;
  }

  protected double getAverageReadLatency() {
    return TimeUnit.MILLISECONDS.convert(TotalGetLatency, TimeUnit.NANOSECONDS) / numGets;
  }

  protected double getReadThroughput() {
    return (numGets / TimeUnit.SECONDS.convert(TotalGetLatency, TimeUnit.NANOSECONDS));
  }

  protected double getWriteThroughput() {
    return (numPuts / TimeUnit.SECONDS.convert(TotalPutLatency, TimeUnit.NANOSECONDS));
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("Metrics{");
    sb.append("TotalGetLatency=").append(TotalGetLatency);
    sb.append(", TotalPutLatency=").append(TotalPutLatency);
    sb.append(", numPuts=").append(numPuts);
    sb.append(", numGets=").append(numGets);
    sb.append(", avgWriteLatency(ms)=").append(getAverageWriteLatency());
    sb.append(", avgReadLatency(ms)=").append(getAverageReadLatency());
    sb.append(", readThroughput(reads/s)=").append(getReadThroughput());
    sb.append(", writeThroughput(writes/s)=").append(getWriteThroughput());
    sb.append('}');
    return sb.toString();
  }
}
