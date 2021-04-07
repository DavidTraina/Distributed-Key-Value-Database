package testing;

import static org.junit.Assert.*;

import client.KVStore;
import client.KVStoreException;
import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import shared.communication.messages.KVMessage;
import shared.communication.security.PropertyStore;

public class ByzantineTest {

  private static Process server;

  @BeforeClass
  public static void startServer() {
    try {
      ProcessBuilder builder =
          new ProcessBuilder("java", "-jar", "m2-server.jar", "50001", "0", "LRU");
      builder.redirectOutput(new File("/dev/null"));
      builder.redirectError(new File("/dev/null"));
      server = builder.start();

      TimeUnit.MILLISECONDS.sleep(100); // Wait for server to start properly
      KVStore kvStore = new KVStore(InetAddress.getLocalHost(), 50001);

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
  public void testKVMessageFromServerDoesNotWork() {
    // Set sender ID to be that of the running server
    PropertyStore.getInstance().setSenderID("localhost:50001");
    try {
      KVStore kvStore = new KVStore(InetAddress.getLocalHost(), 50001);
      kvStore.connect();
      KVMessage response = kvStore.get("hello");
      assertSame(response.getStatus(), KVMessage.StatusType.FAILED);
    } catch (UnknownHostException | KVStoreException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testCannotModifyPropertyStore() {
    String s1 = PropertyStore.getInstance().getSenderID();
    if (s1 == null) {
      s1 = "xyz";
      PropertyStore.getInstance().setSenderID("xyz");
    }
    PropertyStore.getInstance().setSenderID("hello");
    assertEquals(s1, PropertyStore.getInstance().getSenderID());
  }
}
