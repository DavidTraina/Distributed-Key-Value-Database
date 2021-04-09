package testing;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import app_kvECS.ECSClient;
import client.KVStore;
import client.KVStoreException;
import ecs.ECSNode;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import shared.communication.messages.KVMessage;
import shared.communication.security.property_stores.ClientPropertyStore;

/**
 * Test assumes zookeeper is up and running on port 2181. Test assumes localhost password-less ssh
 * is setup. Test assumes server jar has been built
 */
public class ECSAcceptanceTests {
  private ECSClient ecs;
  private final HashMap<String, String> keyValueMap = new HashMap<>();

  @Before
  public void setUp() {
    ArrayList<ECSNode> nodes = new ArrayList<>();
    nodes.add(new ECSNode("127.0.0.1", 10020));
    nodes.add(new ECSNode("127.0.0.1", 10021));
    nodes.add(new ECSNode("127.0.0.1", 10022));
    ecs = new ECSClient(nodes, 2, "127.0.0.1", 2181, true);
    new Thread(ecs).start();
    keyValueMap.clear();

    for (int i = 0; i < 100; i++) {
      String key = TestUtils.createRandomCode(4, "ABCDEFGHIJKLMNOPQRSTUVWXYZ123456789");
      // generate a random integer between 1 and 1000
      int valueLen = new Random().nextInt(10) + 1;
      String value = TestUtils.createRandomCode(valueLen, "ABCDEFGHIJKLMNOPQRSTUVWXYZ123456789");
      keyValueMap.put(key, value);
    }

    ClientPropertyStore.getInstance().setSenderID("client");
  }

  @After
  public void tearDown() {
    ecs.shutdown();
    try {
      TimeUnit.SECONDS.sleep(2);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    ecs.shutDownECS();
  }

  @Test
  public void testCanAddNode() {
    ECSNode addedNode = ecs.addNode();
    assertNotNull(addedNode);
    assertCanConnectAsClientToServer(addedNode);
  }

  @Test
  public void testCanAddThenRemoveNode() {
    ECSNode addedNode = ecs.addNode();
    assertNotNull(addedNode);
    assertCanConnectAsClientToServer(addedNode);

    assertTrue(ecs.removeNode(addedNode.getNodeName()));
  }

  @Test
  public void testDataConsistentAfterAddingAndRemovingNode() {
    ECSNode addedNode = ecs.addNode();
    assertNotNull(addedNode);
    assertCanConnectAsClientToServer(addedNode);
    ecs.start();

    putDataset(addedNode);

    // Wait for data propagation
    TestUtils.waitForSeconds(5);

    assertTrue(ecs.removeNode(addedNode.getNodeName()));
    TestUtils.waitForSeconds(5);

    getDataset(ecs.getMetadata().getNodeRing().get(0));
  }

  private void assertCanConnectAsClientToServer(ECSNode server) {
    try {
      TimeUnit.SECONDS.sleep(1);
      KVStore kvStore =
          new KVStore(InetAddress.getByName(server.getNodeHost()), server.getNodePort());
      kvStore.connect();
    } catch (UnknownHostException | KVStoreException | InterruptedException e) {
      fail();
      e.printStackTrace();
    }
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
            status == KVMessage.StatusType.PUT_SUCCESS
                || status == KVMessage.StatusType.PUT_UPDATE);
      }
    } catch (UnknownHostException | KVStoreException | InterruptedException e) {
      fail();
      e.printStackTrace();
    }
  }

  private void getDataset(ECSNode server) {
    try {
      TimeUnit.SECONDS.sleep(1);
      KVStore kvStore =
          new KVStore(InetAddress.getByName(server.getNodeHost()), server.getNodePort());
      kvStore.connect();

      for (String key : keyValueMap.keySet()) {
        assertSame(kvStore.get(key).getStatus(), KVMessage.StatusType.GET_SUCCESS);
      }
    } catch (UnknownHostException | KVStoreException | InterruptedException e) {
      fail();
      e.printStackTrace();
    }
  }
}
