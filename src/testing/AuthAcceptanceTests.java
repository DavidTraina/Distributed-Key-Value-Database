package testing;

import static org.junit.Assert.*;
import static org.junit.Assert.fail;
import static shared.communication.messages.DataTransferMessage.DataTransferMessageType.*;

import app_kvECS.ECSClient;
import app_kvServer.data.storage.DiskStorage;
import client.KVStore;
import client.KVStoreException;
import com.google.gson.Gson;
import ecs.ECSMetadata;
import ecs.ECSNode;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import shared.communication.ProtocolException;
import shared.communication.messages.DataTransferMessage;
import shared.communication.messages.ECSMessage;
import shared.communication.messages.KVMessage;

public class AuthAcceptanceTests {

  private ECSClient ecs;
  private final HashMap<String, String> keyValueMap = new HashMap<>();

  @Before
  public void setUp() {
    ArrayList<ECSNode> nodes = new ArrayList<>();
    nodes.add(new ECSNode("127.0.0.1", 10020));
    nodes.add(new ECSNode("127.0.0.1", 10021));
    nodes.add(new ECSNode("127.0.0.1", 10022));
    ecs = new ECSClient(nodes, 3, "127.0.0.1", 2181, true);
    new Thread(ecs).start();
    keyValueMap.clear();

    for (int i = 0; i < 100; i++) {
      String key = TestUtils.createRandomCode(4, "ABCDEFGHIJKLMNOPQRSTUVWXYZ123456789");
      // generate a random integer between 1 and 1000
      int valueLen = new Random().nextInt(10) + 1;
      String value = TestUtils.createRandomCode(valueLen, "ABCDEFGHIJKLMNOPQRSTUVWXYZ123456789");
      keyValueMap.put(key, value);
    }
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
  public void testECSMoveDataReplayFails() throws IOException, ProtocolException {
    ECSNode nodeSendingData = ECSMetadata.getInstance().getNodeRing().get(0);
    ecs.start();

    putDataset(nodeSendingData);
    TestUtils.waitForSeconds(2);

    ECSNode nodeToSendData = ECSMetadata.getInstance().getNodeRing().get(1);
    ECSMessage ecsMessage =
        new ECSMessage(
            ECSMessage.ActionType.MOVE_DATA, nodeToSendData, nodeToSendData.getNodeHashRange());
    ecsMessage.calculateAndSetMAC();

    ECSMessage response = (ECSMessage) TestUtils.sendMessageToNode(nodeSendingData, ecsMessage);
    assertEquals(ECSMessage.ActionStatus.ACTION_SUCCESS, response.getStatus());

    ECSMessage replayResponse =
        (ECSMessage) TestUtils.sendMessageToNode(nodeSendingData, ecsMessage);
    assertEquals(ECSMessage.ActionStatus.ACTION_FAILED, replayResponse.getStatus());
  }

  @Test
  public void testDataTransferMessageReplayFails() throws IOException, ProtocolException {
    ecs.start();
    ECSNode nodeToSendData = ECSMetadata.getInstance().getNodeRing().get(1);
    ECSMessage ecsMessage =
        new ECSMessage(
            ECSMessage.ActionType.MOVE_DATA, nodeToSendData, nodeToSendData.getNodeHashRange());
    ecsMessage.calculateAndSetMAC();

    DataTransferMessage dataTransferMessage =
        new DataTransferMessage(
            DATA_TRANSFER_REQUEST, new HashSet<>(), "Transfer nothing please", ecsMessage);

    DataTransferMessage response =
        (DataTransferMessage) TestUtils.sendMessageToNode(nodeToSendData, dataTransferMessage);
    assertEquals(DATA_TRANSFER_SUCCESS, response.getDataTransferMessageType());

    DataTransferMessage response2 =
        (DataTransferMessage) TestUtils.sendMessageToNode(nodeToSendData, dataTransferMessage);
    assertEquals(DATA_TRANSFER_FAILURE, response2.getDataTransferMessageType());
  }

  @Test
  public void testECSMessageWrongOriginRequestFails() throws IOException, ProtocolException {
    ecs.start();
    ECSNode nodeToSendRequest = ECSMetadata.getInstance().getNodeRing().get(0);
    String forgedECSMessageJson =
        "{\"action\":\"MOVE_DATA\",\"dataTransferServer\":{\"nodeHash\":\"B5B201FD539E132457F4CBD773AE8462\",\"name\":\"127.0.0.1:10021\",\"address\":\"127.0.0.1\",\"port\":10021,\"lowerRange\":\"923ED75E8E40DAE41B9776B3B3F26021\"},\"dataTransferHashRange\":[\"923ED75E8E40DAE41B9776B3B3F26021\",\"B5B201FD539E132457F4CBD773AE8462\"],\"senderID\":\"notECS\",\"timestamp\":\"1617920467441\",\"MAC\":\"aNCtdbThLy+zzPOMSy43ZlGOzQrXUahR/xqjwvxusHgG2U4OcNnbnuiJm44KrBvPe7IP+ctoh/HYOIeexdAlfGatB2uUjtBzoMGaSYav3K6idBIoRELNmHl3RE3/P4nA1n+sCqIkXFSk0tXQr8VT60Rbq9e+sW4RRALMdp+cjaM\\u003d\"}";
    ECSMessage forgedECSMessage = new Gson().fromJson(forgedECSMessageJson, ECSMessage.class);

    ECSMessage response =
        (ECSMessage) TestUtils.sendMessageToNode(nodeToSendRequest, forgedECSMessage);
    assertEquals(ECSMessage.ActionStatus.ACTION_FAILED, response.getStatus());
  }

  @Test
  public void testMajorityNotUsedWhenResponsibleNodeFaulty()
      throws IOException, KVStoreException, ProtocolException {
    ecs.start();
    ECSNode nodeToSendData = ECSMetadata.getInstance().getNodeRing().get(1);

    KVStore kvStore = new KVStore(InetAddress.getLocalHost(), 10020);
    kvStore.connect();
    String key = "hello";
    kvStore.put(key, "world");

    TestUtils.waitForSeconds(2);
    ECSNode nodeResponsible = ecs.getMetadata().getNodeBasedOnKey(key);

    // Simulate a failure or malicious main node by deleting its keys
    ECSMessage ecsMessage =
        new ECSMessage(
            ECSMessage.ActionType.MOVE_DATA, nodeToSendData, nodeToSendData.getNodeHashRange());
    ecsMessage.calculateAndSetMAC();

    DataTransferMessage dataTransferMessage =
        new DataTransferMessage(
            DELETE_DATA,
            nodeResponsible.getNodeHashRange(),
            "Delete all your data please",
            ecsMessage);
    dataTransferMessage.setStorageType(DiskStorage.StorageType.SELF);

    DataTransferMessage response =
        (DataTransferMessage) TestUtils.sendMessageToNode(nodeResponsible, dataTransferMessage);
    assertNotNull(response);

    assertEquals(KVMessage.StatusType.GET_ERROR, kvStore.get(key).getStatus());
  }

  @Test
  public void testMajorityWorksWhenOneReplicaFaulty()
      throws IOException, KVStoreException, ProtocolException {
    ecs.start();
    ECSNode nodeToSendData = ECSMetadata.getInstance().getNodeRing().get(1);

    KVStore kvStore = new KVStore(InetAddress.getLocalHost(), 10020);
    kvStore.connect();
    String key = "hello";
    kvStore.put(key, "world");

    TestUtils.waitForSeconds(2);
    ECSNode nodeResponsible = ecs.getMetadata().getNodeBasedOnKey(key);
    ECSNode[] replicas = ecs.getMetadata().getReplicasBasedOnName(nodeResponsible.getNodeName());
    ECSNode replicaToFail = replicas[0];

    // Simulate a failure or malicious main node by deleting its keys
    ECSMessage ecsMessage =
        new ECSMessage(
            ECSMessage.ActionType.MOVE_DATA, nodeToSendData, nodeToSendData.getNodeHashRange());
    ecsMessage.calculateAndSetMAC();

    DataTransferMessage dataTransferMessage =
        new DataTransferMessage(
            DELETE_DATA,
            nodeResponsible.getNodeHashRange(),
            "Delete all your data please",
            ecsMessage);
    dataTransferMessage.setStorageType(DiskStorage.StorageType.REPLICA_1);

    DataTransferMessage response =
        (DataTransferMessage) TestUtils.sendMessageToNode(replicaToFail, dataTransferMessage);
    assertNotNull(response);

    assertEquals("world", kvStore.get(key).getValue());
  }

  @Rule public ExpectedException exceptionRule = ExpectedException.none();

  @Test
  public void testMajorityThrowsErrorWhenTwoReplicaFaulty()
      throws IOException, KVStoreException, ProtocolException {
    String key = "hello";
    exceptionRule.expect(KVStoreException.class);
    exceptionRule.expectMessage(
        "Majority not reached for key: "
            + key
            + " there might be a security breach on the servers.");

    ecs.start();
    ECSNode nodeToSendData = ECSMetadata.getInstance().getNodeRing().get(1);

    KVStore kvStore = new KVStore(InetAddress.getLocalHost(), 10020);
    kvStore.connect();
    kvStore.put(key, "world");

    TestUtils.waitForSeconds(2);
    ECSNode nodeResponsible = ecs.getMetadata().getNodeBasedOnKey(key);
    ECSNode[] replicas = ecs.getMetadata().getReplicasBasedOnName(nodeResponsible.getNodeName());

    // Simulate a failure or malicious main node by deleting its keys
    ECSMessage ecsMessage =
        new ECSMessage(
            ECSMessage.ActionType.MOVE_DATA, nodeToSendData, nodeToSendData.getNodeHashRange());
    ecsMessage.calculateAndSetMAC();

    DataTransferMessage dataTransferMessage =
        new DataTransferMessage(
            DELETE_DATA,
            nodeResponsible.getNodeHashRange(),
            "Delete all your data please",
            ecsMessage);

    for (int i = 0; i < replicas.length; i++) {
      dataTransferMessage.setStorageType(DiskStorage.StorageType.REPLICA_1);
      if (i == 1) dataTransferMessage.setStorageType(DiskStorage.StorageType.REPLICA_2);
      DataTransferMessage response =
          (DataTransferMessage) TestUtils.sendMessageToNode(replicas[i], dataTransferMessage);
      assertNotNull(response);
    }

    kvStore.get(key);
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
