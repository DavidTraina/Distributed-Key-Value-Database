package testing;

import app_kvServer.KVServer;
import app_kvServer.data.SynchronizedKVManager;
import app_kvServer.data.cache.CacheStrategy;
import java.io.IOException;
import junit.framework.Test;
import junit.framework.TestSuite;
import logger.LogSetup;
import org.apache.log4j.Level;

public class AllTests {

  static {
    try {
      new LogSetup("logs/testing/test.log", Level.ERROR);
      SynchronizedKVManager.initialize(10, CacheStrategy.FIFO);
      new KVServer(50000);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static Test suite() {
    TestSuite clientSuite = new TestSuite("Basic Storage ServerTest-Suite");
    clientSuite.addTestSuite(ConnectionTest.class);
    clientSuite.addTestSuite(InteractionTest.class);
    clientSuite.addTestSuite(AdditionalTest.class);
    return clientSuite;
  }
}
