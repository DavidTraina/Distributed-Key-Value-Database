package testing;

import static org.junit.Assert.*;
import static org.junit.Assert.fail;

import app_kvECS.ECSClient;
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
import shared.communication.security.property_stores.ClientPropertyStore;
import shared.communication.security.property_stores.ECSPropertyStore;

/**
 * Test assumes zookeeper is up and running on port 2181. Test assumes localhost password-less ssh
 * is setup. Test assumes server jar has been built
 */
public class ReplicationAcceptanceTest {
  private ECSClient ecs;
  private final HashMap<String, String> keyValueMap = new HashMap<>();

  @Before
  public void setUp() throws NoSuchFieldException, IllegalAccessException {
    ArrayList<ECSNode> nodes = new ArrayList<>();
    nodes.add(new ECSNode("127.0.0.1", 10020));
    nodes.add(new ECSNode("127.0.0.1", 10021));
    nodes.add(new ECSNode("127.0.0.1", 10022));
    nodes.add(new ECSNode("127.0.0.1", 10023));
    ecs = new ECSClient(nodes, 3, "127.0.0.1", 2181, false);
    new Thread(ecs).start();

    for (int i = 0; i < 100; i++) {
      String key = createRandomCode(4, "ABCDEFGHIJKLMNOPQRSTUVWXYZ123456789");
      // generate a random integer between 1 and 1000
      int valueLen = new Random().nextInt(999) + 1;
      String value = createRandomCode(valueLen, "ABCDEFGHIJKLMNOPQRSTUVWXYZ123456789");
      keyValueMap.put(key, value);
    }

    ClientPropertyStore.getInstance().setSenderID("client");
    ECSPropertyStore.getInstance().setSenderID("ecs");
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
    waitForSeconds(5);

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

    // Give time for data movement
    waitForSeconds(3);

    ECSNode addedNode = ecs.addNode();
    assertNotNull(addedNode);

    // Give time for data movement
    waitForSeconds(10);

    ecs.start();
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
    waitForSeconds(10);

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

    waitForSeconds(5);

    getDatasetViaSocket(ecs.getMetadata().getNodeRing().get(0));
  }

  @Test
  public void testECSRecoversNodeWhenCrashed() {
    ecs.start();

    String nodeToCrash = ecs.getMetadata().getNodeRing().get(0).getNodeName();
    killServer(ecs.getMetadata().getNodeRing().get(0).getNodePort());

    waitForSeconds(6);

    assertEquals(ecs.getMetadata().getNodeRing().get(0).getNodeName(), nodeToCrash);
  }

  @Test
  public void testCanReadFromReplicasAfterCrashingNode() {
    ecs.start();
    putDataset(ecs.getMetadata().getNodeRing().get(0));

    waitForSeconds(3);

    killServer(ecs.getMetadata().getNodeRing().get(0).getNodePort());

    waitForSeconds(20);

    ecs.start();

    getDatasetViaSocket(ecs.getMetadata().getNodeRing().get(0));
    getDatasetViaSocket(ecs.getMetadata().getNodeRing().get(1));
    getDatasetViaSocket(ecs.getMetadata().getNodeRing().get(2));
  }

  @Test
  public void testCanReadFromReplicasAfterCrashingTwoNodes() {
    // This test is a bit flaky, there might be some timing related logic causing it to fail
    // sometimes
    ecs.start();
    putDataset(ecs.getMetadata().getNodeRing().get(0));

    waitForSeconds(3);

    killServer(ecs.getMetadata().getNodeRing().get(0).getNodePort());
    killServer(ecs.getMetadata().getNodeRing().get(1).getNodePort());

    waitForSeconds(15);

    ecs.start();

    getDatasetViaSocket(ecs.getMetadata().getNodeRing().get(0));
    getDatasetViaSocket(ecs.getMetadata().getNodeRing().get(1));
    getDatasetViaSocket(ecs.getMetadata().getNodeRing().get(2));
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
        new ProcessBuilder("pkill", "-f", String.format("m2-server.jar %s", serverPort));
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
        KVMessage messageToSend = new KVMessage(key, null, KVMessage.StatusType.GET).calculateMAC();

        Protocol.sendMessage(outputStream, messageToSend);
        KVMessage response = (KVMessage) Protocol.receiveMessage(inputStream);
        assertSame(KVMessage.StatusType.GET_SUCCESS, response.getStatus());
      }
    } catch (InterruptedException | IOException | ProtocolException e) {
      fail("Error in get dataset");
      e.printStackTrace();
    }
  }
}
