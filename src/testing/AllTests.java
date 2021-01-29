package testing;

import app_kvServer.KVServer;
import app_kvServer.data.SynchronizedKVManager;
import app_kvServer.data.cache.CacheStrategy;
import java.io.File;
import java.io.IOException;
import junit.framework.Test;
import junit.framework.TestSuite;
import logger.LogSetup;
import org.apache.log4j.Level;

public class AllTests {

  static {
    try {
      // remove existing storage to start fresh
      File storage = new File("KeyValueData.txt");
      System.out.println(storage.getAbsolutePath());
      if (storage.exists() && !storage.delete()) {
        throw new IOException("Unable to delete file " + storage.getAbsolutePath());
      }

      new LogSetup("logs/testing/test.log", Level.INFO);
      SynchronizedKVManager.initialize(0, CacheStrategy.FIFO);

      // command to start KVServer changed to reflect current class types and structure
      new Thread(new KVServer(50000)).start();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static Test suite() {
    TestSuite clientSuite = new TestSuite("Basic Storage ServerTest-Suite");
    clientSuite.addTestSuite(ConnectionTest.class);
    clientSuite.addTestSuite(InteractionTest.class);
    clientSuite.addTestSuite(KVMessageTest.class);
    return clientSuite;
  }
}
