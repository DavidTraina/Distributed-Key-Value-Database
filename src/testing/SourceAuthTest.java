package testing;

import client.KVStore;
import client.KVStoreException;
import java.io.File;
import java.net.InetAddress;
import java.security.spec.InvalidKeySpecException;
import java.util.concurrent.TimeUnit;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import shared.communication.security.*;
import shared.communication.security.keys.ClientPublicKey;
import shared.communication.security.property_stores.ServerPropertyStore;

public class SourceAuthTest {

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

  @Before
  public void setUp() throws InvalidKeySpecException {
    // Private key is init in KvStore i.e BeforeClass
    ServerPropertyStore.getInstance()
        .setClientPublicKey(KeyLoader.getPublicKey(ClientPublicKey.base64EncodedPublicKey));
  }
}
