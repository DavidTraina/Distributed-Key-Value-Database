package testing;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import client.KVStore;
import client.KVStoreException;
import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class ConnectionTest {

  private static Process server;

  @BeforeClass
  public static void startServer() {
    try {
      ProcessBuilder builder =
          new ProcessBuilder("java", "-jar", "m2-server.jar", "50000", "1", "LRU");
      builder.redirectOutput(new File("out.txt"));
      builder.redirectError(new File("out.txt"));
      server = builder.start();

      TimeUnit.MILLISECONDS.sleep(100); // Wait for server to start properly
      KVStore kvStore = new KVStore(InetAddress.getLocalHost(), 50000);

      // Loop to ensure server is up before starting tests
      while (true) {
        try {
          kvStore.connect();
          break;
        } catch (KVStoreException e) {
          TimeUnit.MILLISECONDS.sleep(100);
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @AfterClass
  public static void stopServer() {
    server.destroy();
    try {
      server.waitFor();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testConnectionSuccess() {

    Exception ex = null;
    try {
      KVStore kvClient = new KVStore(InetAddress.getByName("localhost"), 50000);
      kvClient.connect();
    } catch (Exception e) {
      ex = e;
    }

    assertNull(ex);
  }

  @Test
  public void testUnknownHost() {
    Exception ex = null;
    try {
      KVStore kvClient = new KVStore(InetAddress.getByName("unknown"), 50000);
      kvClient.connect();
    } catch (Exception e) {
      ex = e;
    }

    // changed assert statement to comply with existing error handling strategy
    assertTrue(ex instanceof UnknownHostException);
  }

  @Test
  public void testIllegalPort() {
    Exception ex = null;

    try {
      KVStore kvClient = new KVStore(InetAddress.getByName("localhost"), 123456789);
      kvClient.connect();
    } catch (Exception e) {
      ex = e;
    }

    assertTrue(ex instanceof IllegalArgumentException);
  }
}
