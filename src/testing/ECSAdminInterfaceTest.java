package testing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import client.KVStore;
import client.KVStoreException;
import ecs.ECSNode;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import shared.communication.Protocol;
import shared.communication.ProtocolException;
import shared.communication.messages.ClientKVMessage;
import shared.communication.messages.ECSMessage;

public class ECSAdminInterfaceTest {

  private static final int testServerPort = 49999;
  private Process testServer;
  private KVStore kvStore;
  private Socket clientSocket;
  private OutputStream output;
  private InputStream input;

  @Before
  public void setUp() {
    try {
      File storage = new File("KeyValueData_localhost:" + testServerPort + ".txt");
      if (storage.exists() && !storage.delete()) {
        throw new IOException("Unable to delete file " + storage.getAbsolutePath());
      }

      ProcessBuilder builder =
          new ProcessBuilder(
              "java", "-jar", "m2-server.jar", String.valueOf(testServerPort), "1", "LRU");
      builder.redirectOutput(new File("/dev/null"));
      builder.redirectError(new File("/dev/null"));
      testServer = builder.start();

      TimeUnit.MILLISECONDS.sleep(100); // Wait for server to start properly
      kvStore = new KVStore(InetAddress.getLocalHost(), testServerPort);

      // Loop to ensure server is up before starting tests
      while (true) {
        try {
          kvStore.connect();
          break;
        } catch (KVStoreException e) {
          TimeUnit.MILLISECONDS.sleep(100);
        }
      }

      this.clientSocket = new Socket("localhost", testServerPort);
      this.output = clientSocket.getOutputStream();
      this.input = clientSocket.getInputStream();

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @After
  public void tearDown() {
    try {
      kvStore.disconnect();
      clientSocket.close();

      testServer.destroy();
      testServer.waitFor();
    } catch (KVStoreException | InterruptedException | IOException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testCorrectReplyWhenServerStopped() {
    try {
      sendECSMessageToTestServer(new ECSMessage(ECSMessage.ActionType.STOP));
      KVStore kvStore2 = new KVStore(InetAddress.getLocalHost(), testServerPort);
      kvStore2.connect();

      ClientKVMessage reply = kvStore.put("ahdj", "jalsd");
      ClientKVMessage reply2 = kvStore2.put("ahdj", "jalsd");

      assertEquals(ClientKVMessage.StatusType.SERVER_STOPPED, reply.getStatus());
      assertEquals(ClientKVMessage.StatusType.SERVER_STOPPED, reply2.getStatus());
    } catch (KVStoreException e) {
      fail("Problem sending KVMessage");
      e.printStackTrace();
    } catch (UnknownHostException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testCorrectReplyWhenServerStoppedThenStarted() {
    try {
      sendECSMessageToTestServer(new ECSMessage(ECSMessage.ActionType.STOP));
      sendECSMessageToTestServer(new ECSMessage(ECSMessage.ActionType.START));

      ClientKVMessage reply = kvStore.put("ahdj", "jalsd");
      assertEquals(ClientKVMessage.StatusType.PUT_SUCCESS, reply.getStatus());
    } catch (KVStoreException e) {
      fail("Problem sending KVMessage");
      e.printStackTrace();
    }
  }

  @Test
  public void testCorrectReplyWhenServerHasWriteLock() {
    try {
      sendECSMessageToTestServer(new ECSMessage(ECSMessage.ActionType.LOCK_WRITE));
      KVStore kvStore2 = new KVStore(InetAddress.getLocalHost(), testServerPort);
      kvStore2.connect();

      ClientKVMessage reply = kvStore.put("ahdj", "jalsd");
      ClientKVMessage reply2 = kvStore2.put("ahdj", "jalsd");

      assertEquals(ClientKVMessage.StatusType.SERVER_WRITE_LOCK, reply.getStatus());
      assertEquals(ClientKVMessage.StatusType.SERVER_WRITE_LOCK, reply2.getStatus());
    } catch (KVStoreException e) {
      fail("Problem sending KVMessage");
      e.printStackTrace();
    } catch (UnknownHostException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testCorrectReplyWhenServerHasWriteLockThenUnlocked() {
    try {
      sendECSMessageToTestServer(new ECSMessage(ECSMessage.ActionType.LOCK_WRITE));
      sendECSMessageToTestServer(new ECSMessage(ECSMessage.ActionType.UNLOCK_WRITE));

      ClientKVMessage reply = kvStore.put("ahdj", "jalsd");
      assertEquals(ClientKVMessage.StatusType.PUT_SUCCESS, reply.getStatus());
    } catch (KVStoreException e) {
      fail("Problem sending KVMessage");
      e.printStackTrace();
    }
  }

  @Test
  public void testProcessDeadWhenServerShutDown() {
    try {
      sendECSMessageToTestServer(new ECSMessage(ECSMessage.ActionType.SHUTDOWN));
      assertTrue(testServer.waitFor(1, TimeUnit.SECONDS));

    } catch (InterruptedException e) {
      fail("Problem waiting for termination of server");
      e.printStackTrace();
    }
  }

  @Test
  public void testMoveDataWorksBetweenTwoServers() {
    try {
      ProcessBuilder builder =
          new ProcessBuilder("java", "-jar", "m2-server.jar", "50000", "1", "LRU");
      builder.redirectOutput(new File("/dev/null"));
      builder.redirectError(new File("/dev/null"));
      Process server2 = builder.start();
      TimeUnit.SECONDS.sleep(2);

      // Keys with their hashes (in sorted order)
      kvStore.put("strawberry", "STRAWBERRY"); // MD5 hash 495BF9840649EE1EC953D99F8E769889
      kvStore.put("banana", "BANANA"); // MD5 hash 72B302BF297A228A75730123EFEF7C41
      kvStore.put("pear", "PEAR"); // MD5 hash 8893DC16B1B2534BAB7B03727145A2BB
      kvStore.put("apple", "APPLE"); // MD5 hash 1F3870BE274F6C49B3E31A0C6728957F
      kvStore.put("blueberry", "BLUEBERRY"); // MD5 hash 8BEA7325CB48514196063A1F74CF18A4

      // strawberry and banana included in this range
      String[] hashRange = {"495BF9840649EE1EC953D99F8E769889", "8893DC16B1B2534BAB7B03727145A2BB"};

      ECSMessage moveData =
          new ECSMessage(
              ECSMessage.ActionType.MOVE_DATA, new ECSNode("localhost", 50000), hashRange);

      sendECSMessageToTestServer(moveData);

      KVStore originalServerClient = new KVStore(InetAddress.getLocalHost(), 50000);
      originalServerClient.connect();

      // Check keys that should be transferred to other server
      assertEquals("STRAWBERRY", originalServerClient.get("strawberry").getValue());
      assertEquals("BANANA", originalServerClient.get("banana").getValue());

      // Ensure testServer does not have transferred keys
      assertEquals(ClientKVMessage.StatusType.GET_ERROR, kvStore.get("strawberry").getStatus());
      assertEquals(ClientKVMessage.StatusType.GET_ERROR, kvStore.get("banana").getStatus());

      // Ensure testServer has keys not in range
      assertEquals("PEAR", kvStore.get("pear").getValue());
      assertEquals("APPLE", kvStore.get("apple").getValue());
      assertEquals("BLUEBERRY", kvStore.get("blueberry").getValue());

      server2.destroy();
      server2.waitFor();

    } catch (KVStoreException e) {
      fail("Problem sending KVMessage");
      e.printStackTrace();
    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
    }
  }

  private void sendECSMessageToTestServer(ECSMessage message) {
    try {
      Protocol.sendMessage(output, message);
      ECSMessage response = (ECSMessage) Protocol.receiveMessage(input);
      assertEquals(ECSMessage.ActionStatus.ACTION_SUCCESS, response.getStatus());
    } catch (IOException e) {
      fail("Problem sending ECSMessage");
      e.printStackTrace();
    } catch (ProtocolException e) {
      fail("Problem in receiveMessage");
      e.printStackTrace();
    }
  }
}
