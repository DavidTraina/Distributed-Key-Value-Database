package testing;

import static org.junit.Assert.*;
import static org.junit.Assert.fail;

import app_kvECS.ECSClient;
import app_kvServer.data.SynchronizedKVManager;
import app_kvServer.data.cache.CacheStrategy;
import client.KVStore;
import client.KVStoreException;
import ecs.ECSMetadata;
import ecs.ECSNode;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import shared.communication.Protocol;
import shared.communication.ProtocolException;
import shared.communication.messages.KVMessage;

/**
 * Test assumes zookeeper is up and running on port 2181. Test assumes localhost password-less ssh
 * is setup. Test assumes server jar has been built
 */
public class ReplicationAcceptanceTest {
  private ECSClient ecs;
  private final HashMap<String, String> keyValueMap = new HashMap<>();

  @Before
  public void setUp() throws NoSuchFieldException, IllegalAccessException {
    resetSingleton();
    ArrayList<ECSNode> nodes = new ArrayList<>();
    nodes.add(new ECSNode("127.0.0.1", 10020));
    nodes.add(new ECSNode("127.0.0.1", 10021));
    nodes.add(new ECSNode("127.0.0.1", 10022));
    nodes.add(new ECSNode("127.0.0.1", 10023));
    ecs = new ECSClient(nodes, 3, "127.0.0.1", 2181);

    for (int i = 0; i < 100; i++) {
      String key = createRandomCode(4, "ABCDEFGHIJKLMNOPQRSTUVWXYZ123456789");
      // generate a random integer between 0 and 1000
      int valueLen = new Random().nextInt(1000);
      String value = createRandomCode(valueLen, "ABCDEFGHIJKLMNOPQRSTUVWXYZ123456789");
      keyValueMap.put(key, value);
    }
  }

  private void resetSingleton()
      throws SecurityException, NoSuchFieldException, IllegalArgumentException,
          IllegalAccessException {
    Field instance = SynchronizedKVManager.class.getDeclaredField("INSTANCE");
    instance.setAccessible(true);
    instance.set(null, null);
    SynchronizedKVManager.initialize(0, CacheStrategy.LRU, "localhost:48");

    Field instance2 = ECSMetadata.class.getDeclaredField("singletonECSMetadata");
    instance2.setAccessible(true);
    instance2.set(null, null);

    ArrayList<ECSNode> allNodes = new ArrayList<>();
    ECSMetadata.initialize(allNodes);
  }

  @After
  public void tearDown() {
    ecs.shutdown();
    waitForSeconds(2);
    ecs.shutDownECS();
  }

  @Test
  public void testCanReadFromReplicas() {
    ecs.start();
    putDataset(ecs.getMetadata().getNodeRing().get(0));

    getDatasetViaSocket(ecs.getMetadata().getNodeRing().get(0));
    getDatasetViaSocket(ecs.getMetadata().getNodeRing().get(1));
    getDatasetViaSocket(ecs.getMetadata().getNodeRing().get(2));
  }

  @Test
  public void testCanReadFromReplicasAfterRemovingNode() {
    ECSNode addedNode = ecs.addNode();
    assertNotNull(addedNode);
    ecs.start();
    putDataset(ecs.getMetadata().getNodeRing().get(0));
    waitForSeconds(2);

    assertTrue(ecs.removeNode(addedNode.getNodeName()));
    waitForSeconds(5);

    getDatasetViaSocket(ecs.getMetadata().getNodeRing().get(0));
    getDatasetViaSocket(ecs.getMetadata().getNodeRing().get(1));
    getDatasetViaSocket(ecs.getMetadata().getNodeRing().get(2));
  }

  @Test
  public void testCanReadFromReplicasAfterGoingFrom2NodesTo3Nodes() {
    assertTrue(ecs.removeNode(ecs.getMetadata().getNodeRing().get(0).getNodeName()));

    ecs.start();
    putDataset(ecs.getMetadata().getNodeRing().get(0));

    ECSNode addedNode = ecs.addNode();
    assertNotNull(addedNode);
    ecs.start();

    // Give time for data movement
    waitForSeconds(5);

    getDatasetViaSocket(ecs.getMetadata().getNodeRing().get(0));
    getDatasetViaSocket(ecs.getMetadata().getNodeRing().get(1));
    getDatasetViaSocket(ecs.getMetadata().getNodeRing().get(2));
  }

  @Test
  public void testCanReadFromReplicasAfterManyChanges() {
    assertTrue(ecs.removeNode(ecs.getMetadata().getNodeRing().get(0).getNodeName()));
    waitForSeconds(5);

    ecs.start();
    putDataset(ecs.getMetadata().getNodeRing().get(0));

    // Wait for eventual consistency
    waitForSeconds(5);

    getDatasetViaSocket(ecs.getMetadata().getNodeRing().get(0));
    getDatasetViaSocket(ecs.getMetadata().getNodeRing().get(1));

    ECSNode addedNode = ecs.addNode();
    assertNotNull(addedNode);
    ecs.start();

    // Give time for data movement
    waitForSeconds(5);

    getDatasetViaSocket(ecs.getMetadata().getNodeRing().get(0));
    getDatasetViaSocket(ecs.getMetadata().getNodeRing().get(1));
    getDatasetViaSocket(ecs.getMetadata().getNodeRing().get(2));
  }

  @Test
  public void testCanReadFromReplicasAfterRemovingTwoNodes() {
    ecs.start();
    putDataset(ecs.getMetadata().getNodeRing().get(0));

    assertTrue(ecs.removeNode(ecs.getMetadata().getNodeRing().get(0).getNodeName()));
    assertTrue(ecs.removeNode(ecs.getMetadata().getNodeRing().get(1).getNodeName()));

    getDatasetViaSocket(ecs.getMetadata().getNodeRing().get(0));
  }

  @Test
  public void testCanReadFromReplicasAfterCrashingNode() {
    ECSNode addedNode = ecs.addNode();
    assertNotNull(addedNode);
    ecs.start();
    putDataset(ecs.getMetadata().getNodeRing().get(0));

    killServer(addedNode.getNodePort());

    getDatasetViaSocket(ecs.getMetadata().getNodeRing().get(0));
    getDatasetViaSocket(ecs.getMetadata().getNodeRing().get(1));
    getDatasetViaSocket(ecs.getMetadata().getNodeRing().get(2));
  }

  @Test
  public void testCanReadFromReplicasAfterCrashingTwoNodes() {
    ecs.start();
    putDataset(ecs.getMetadata().getNodeRing().get(0));

    killServer(ecs.getMetadata().getNodeRing().get(0).getNodePort());
    killServer(ecs.getMetadata().getNodeRing().get(1).getNodePort());

    getDatasetViaSocket(ecs.getMetadata().getNodeRing().get(0));
  }

  private void waitForSeconds(int seconds) {
    try {
      TimeUnit.SECONDS.sleep(seconds);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  private void killServer(int serverPort) {
    ProcessBuilder builder =
        new ProcessBuilder("pkill", "-ef", String.format("m2-server.jar %s", serverPort));
    builder.redirectOutput(new File("out2.txt"));
    builder.redirectError(new File("out2.txt"));
    try {
      // Wait to ensure all replicas consistent
      TimeUnit.SECONDS.sleep(5);
      builder.start();
      // Wait to ensure, recovery takes place
      TimeUnit.SECONDS.sleep(5);
    } catch (IOException | InterruptedException e) {
      e.printStackTrace();
    }
  }

  // Copied from stackoverflow:
  // https://stackoverflow.com/questions/39222044/generate-random-string-in-java
  private String createRandomCode(int codeLength, String id) {
    return new SecureRandom()
        .ints(codeLength, 0, id.length())
        .mapToObj(id::charAt)
        .map(Object::toString)
        .collect(Collectors.joining());
  }

  private void putDataset(ECSNode server) {
    try {
      TimeUnit.SECONDS.sleep(1);
      KVStore kvStore =
          new KVStore(InetAddress.getByName(server.getNodeHost()), server.getNodePort());
      kvStore.connect();

      for (String key : keyValueMap.keySet()) {
        KVMessage.StatusType status = kvStore.put(key, keyValueMap.get(key)).getStatus();
        assertTrue(
            status.name(),
            status == KVMessage.StatusType.PUT_SUCCESS
                || status == KVMessage.StatusType.PUT_UPDATE);
      }
    } catch (UnknownHostException | KVStoreException | InterruptedException e) {
      fail();
      e.printStackTrace();
    }
  }

  // Method does not use KVStore to prevent redirection
  private void getDatasetViaSocket(ECSNode server) {
    try {
      TimeUnit.SECONDS.sleep(1);

      Socket serverSocket =
          new Socket(InetAddress.getByName(server.getNodeHost()), server.getNodePort());
      InputStream inputStream = serverSocket.getInputStream();
      OutputStream outputStream = serverSocket.getOutputStream();

      for (String key : keyValueMap.keySet()) {
        Protocol.sendMessage(outputStream, new KVMessage(key, null, KVMessage.StatusType.GET));
        KVMessage response = (KVMessage) Protocol.receiveMessage(inputStream);
        assertSame(KVMessage.StatusType.GET_SUCCESS, response.getStatus());
      }
    } catch (InterruptedException | IOException | ProtocolException e) {
      fail("Error in get dataset");
      e.printStackTrace();
    }
  }
}
