package testing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static shared.communication.messages.DataTransferMessage.DataTransferMessageType.DATA_TRANSFER_REQUEST;

import app_kvServer.data.SynchronizedKVManager;
import app_kvServer.data.cache.CacheStrategy;
import ecs.ECSMetadata;
import ecs.ECSNode;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Before;
import org.junit.Test;
import shared.communication.messages.ClientKVMessage;
import shared.communication.messages.DataTransferMessage;

public class SynchronizedKVManagerTest {

  @Before
  public void resetSingleton()
      throws SecurityException, NoSuchFieldException, IllegalArgumentException,
          IllegalAccessException {
    Field instance = SynchronizedKVManager.class.getDeclaredField("INSTANCE");
    instance.setAccessible(true);
    instance.set(null, null);
    SynchronizedKVManager.initialize(0, CacheStrategy.LRU, "localhost:48");

    Field instance2 = ECSMetadata.class.getDeclaredField("singletonECSMetadata");
    instance2.setAccessible(true);
    instance2.set(null, null);

    ECSNode loneNode = new ECSNode("localhost", 48);
    loneNode.setLowerRange(loneNode.getNodeHash());
    ArrayList<ECSNode> allNodes = new ArrayList<>();
    allNodes.add(loneNode);
    ECSMetadata.initialize(allNodes);
  }

  @Test
  public void testHandleDataTransfer() {
    SynchronizedKVManager skvmngr = SynchronizedKVManager.getInstance();

    HashMap<String, String> dataToTransfer = new HashMap<>();
    String key1 = RandomStringUtils.randomAlphanumeric(5);
    String key2 = RandomStringUtils.randomAlphanumeric(5);
    String keyPut = RandomStringUtils.randomAlphanumeric(5);
    dataToTransfer.put(key1, "abcde");
    dataToTransfer.put(key2, "qwerty");

    DataTransferMessage dtmsg =
        new DataTransferMessage(DATA_TRANSFER_REQUEST, dataToTransfer, "test");
    ClientKVMessage clientKvMessagePut =
        new ClientKVMessage(keyPut, "testing123", ClientKVMessage.StatusType.PUT);
    ClientKVMessage clientKvMessagePutGet =
        new ClientKVMessage(keyPut, null, ClientKVMessage.StatusType.GET);
    ClientKVMessage clientKvMessageKey1 =
        new ClientKVMessage(key1, null, ClientKVMessage.StatusType.GET);
    ClientKVMessage clientKvMessageKey2 =
        new ClientKVMessage(key2, null, ClientKVMessage.StatusType.GET);

    skvmngr.handleClientRequest(clientKvMessagePut);
    assertEquals("testing123", skvmngr.handleClientRequest(clientKvMessagePutGet).getValue());

    skvmngr.handleDataTransfer(dtmsg);
    assertEquals("abcde", skvmngr.handleClientRequest(clientKvMessageKey1).getValue());
    assertEquals("qwerty", skvmngr.handleClientRequest(clientKvMessageKey2).getValue());
    assertEquals("testing123", skvmngr.handleClientRequest(clientKvMessagePutGet).getValue());
  }

  @Test
  public void testPartitionDatabaseAndGetKeysInRange() {
    SynchronizedKVManager skvmngr = SynchronizedKVManager.getInstance();

    String key1 = "qwfgth"; // MD5 hash: 338d66d0d3b47cb2a94876d20024bd6e
    String key2 = "kjkljl"; // MD5 hash: b3ec1f1c725bb7e274fb59854cca6c9d
    String key3 = "jlkhh"; // MD5 hash: cfeb063f8604de1a1784b932a19d3c91

    ClientKVMessage clientKvMessageKey1 =
        new ClientKVMessage(key1, "abc", ClientKVMessage.StatusType.PUT);
    ClientKVMessage clientKvMessageKey2 =
        new ClientKVMessage(key2, "def", ClientKVMessage.StatusType.PUT);
    ClientKVMessage clientKvMessageKey3 =
        new ClientKVMessage(key3, "ghi", ClientKVMessage.StatusType.PUT);

    ClientKVMessage clientKvMessageKey1Get =
        new ClientKVMessage(key1, null, ClientKVMessage.StatusType.GET);
    ClientKVMessage clientKvMessageKey2Get =
        new ClientKVMessage(key2, null, ClientKVMessage.StatusType.GET);
    ClientKVMessage clientKvMessageKey3Get =
        new ClientKVMessage(key3, null, ClientKVMessage.StatusType.GET);

    skvmngr.handleClientRequest(clientKvMessageKey1);
    skvmngr.handleClientRequest(clientKvMessageKey2);
    skvmngr.handleClientRequest(clientKvMessageKey3);

    // Ensure all keys on disk
    assertEquals("abc", skvmngr.handleClientRequest(clientKvMessageKey1Get).getValue());
    assertEquals("def", skvmngr.handleClientRequest(clientKvMessageKey2Get).getValue());
    assertEquals("ghi", skvmngr.handleClientRequest(clientKvMessageKey3Get).getValue());

    // partition by Key1 hash upto Key2 hash (should be not included in partition)
    String[] hashRange =
        new String[] {
          "338d66d0d3b47cb2a94876d20024bd6e".toUpperCase(),
          "b3ec1f1c725bb7e274fb59854cca6c9d".toUpperCase()
        };
    DataTransferMessage dtmsg = skvmngr.partitionDatabaseAndGetKeysInRange(hashRange);

    // key1 should not be on disk anymore, key2 and key3 should be
    assertEquals(
        ClientKVMessage.StatusType.GET_ERROR,
        skvmngr.handleClientRequest(clientKvMessageKey1Get).getStatus());
    assertEquals(
        ClientKVMessage.StatusType.GET_SUCCESS,
        skvmngr.handleClientRequest(clientKvMessageKey2Get).getStatus());
    assertEquals(
        ClientKVMessage.StatusType.GET_SUCCESS,
        skvmngr.handleClientRequest(clientKvMessageKey3Get).getStatus());

    // key1 should be part of the payload, key2 and key3 should not
    assertTrue(dtmsg.getPayload().containsKey(key1));
    assertFalse(dtmsg.getPayload().containsKey(key2));
    assertFalse(dtmsg.getPayload().containsKey(key3));
  }
}