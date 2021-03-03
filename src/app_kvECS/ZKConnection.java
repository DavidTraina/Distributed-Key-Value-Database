package app_kvECS;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import org.apache.log4j.Logger;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public class ZKConnection {
  private static final Logger logger = Logger.getLogger(ZKConnection.class);
  CountDownLatch connectionLatch = new CountDownLatch(1);
  private ZooKeeper zoo;
  private final int x = 10;

  public ZooKeeper connect(String host, int port) throws IOException, InterruptedException {
    zoo =
        new ZooKeeper(
            host + ":" + port,
            2000,
            event -> {
              logger.info("New Zookeeper Event -> " + event.toString());
              if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                connectionLatch.countDown();
              }
            });

    connectionLatch.await();
    return zoo;
  }

  public void close() throws InterruptedException {
    zoo.close();
  }
}
