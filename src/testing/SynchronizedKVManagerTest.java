package testing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static shared.communication.messages.DataTransferMessage.DataTransferMessageType.DATA_TRANSFER_REQUEST;

import app_kvECS.ECSClient;
import app_kvServer.data.SynchronizedKVManager;
import app_kvServer.data.cache.CacheStrategy;
import app_kvServer.data.storage.StorageUnit;
import client.KVStore;
import ecs.ECSMetadata;
import ecs.ECSNode;
import java.lang.reflect.Field;
import java.security.spec.InvalidKeySpecException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.UUID;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Before;
import org.junit.Test;
import shared.communication.messages.DataTransferMessage;
import shared.communication.messages.ECSMessage;
import shared.communication.messages.KVMessage;
import shared.communication.security.KeyLoader;
import shared.communication.security.keys.ClientPublicKey;
import shared.communication.security.keys.ECSPublicKey;
import shared.communication.security.property_stores.ECSPropertyStore;
import shared.communication.security.property_stores.ServerPropertyStore;

public class SynchronizedKVManagerTest {

  @Before
  public void resetSingleton()
      throws SecurityException, NoSuchFieldException, IllegalArgumentException,
          IllegalAccessException {
    Field instance = SynchronizedKVManager.class.getDeclaredField("INSTANCE");
    instance.setAccessible(true);
    instance.set(null, null);
    SynchronizedKVManager.initialize(0, CacheStrategy.LRU, "localhost:48", false);

    Field instance2 = ECSMetadata.class.getDeclaredField("singletonECSMetadata");
    instance2.setAccessible(true);
    instance2.set(null, null);

    ECSNode loneNode = new ECSNode("localhost", 48);
    loneNode.setLowerRange(loneNode.getNodeHash());
    ArrayList<ECSNode> allNodes = new ArrayList<>();
    allNodes.add(loneNode);
    ECSMetadata.initialize(allNodes);

    KVStore.initializeClientPrivateKey();
    ECSClient.initializePrivateKey();
    ECSPropertyStore.getInstance().setSenderID("ecs");
  }

  @Before
  public void setUp() throws InvalidKeySpecException {
    ServerPropertyStore.getInstance()
        .setClientPublicKey(KeyLoader.getPublicKey(ClientPublicKey.base64EncodedPublicKey));
    ServerPropertyStore.getInstance()
        .setECSPublicKey(KeyLoader.getPublicKey(ECSPublicKey.base64EncodedPublicKey));
  }

  @Test
  public void testHandleDataTransfer() {
    ECSMessage message = new ECSMessage(ECSMessage.ActionType.MOVE_DATA, ECSMetadata.getInstance());
    SynchronizedKVManager skvmngr = SynchronizedKVManager.getInstance();

    HashSet<StorageUnit> dataToTransfer = new HashSet<>();
    String key1 = RandomStringUtils.randomAlphanumeric(5);
    String key2 = RandomStringUtils.randomAlphanumeric(5);
    String keyPut = RandomStringUtils.randomAlphanumeric(5);

    KVMessage put1 = new KVMessage(key1, "abcde", UUID.randomUUID(), KVMessage.StatusType.PUT);
    KVMessage put2 = new KVMessage(key2, "qwerty", UUID.randomUUID(), KVMessage.StatusType.PUT);
    put1.calculateMAC();
    put2.calculateMAC();

    dataToTransfer.add(new StorageUnit(key1, "abcde", put1.getUniqueID(), put1.getMAC()));
    dataToTransfer.add(new StorageUnit(key2, "qwerty", put2.getUniqueID(), put2.getMAC()));

    DataTransferMessage dtmsg =
        new DataTransferMessage(DATA_TRANSFER_REQUEST, dataToTransfer, "test", message);
    KVMessage clientKvMessagePut =
        new KVMessage(keyPut, "testing123", UUID.randomUUID(), KVMessage.StatusType.PUT);
    KVMessage clientKvMessagePutGet =
        new KVMessage(keyPut, null, clientKvMessagePut.getClientId(), KVMessage.StatusType.GET);
    KVMessage clientKvMessageKey1 =
        new KVMessage(key1, null, put1.getClientId(), KVMessage.StatusType.GET);
    KVMessage clientKvMessageKey2 =
        new KVMessage(key2, null, put2.getClientId(), KVMessage.StatusType.GET);

    clientKvMessagePut.calculateMAC();
    clientKvMessagePutGet.calculateMAC();
    clientKvMessageKey1.calculateMAC();
    clientKvMessageKey2.calculateMAC();

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

    final UUID clientId = UUID.randomUUID();
    KVMessage clientKvMessageKey1 = new KVMessage(key1, "abc", clientId, KVMessage.StatusType.PUT);
    KVMessage clientKvMessageKey2 = new KVMessage(key2, "def", clientId, KVMessage.StatusType.PUT);
    KVMessage clientKvMessageKey3 = new KVMessage(key3, "ghi", clientId, KVMessage.StatusType.PUT);

    KVMessage clientKvMessageKey1Get =
        new KVMessage(key1, null, clientId, KVMessage.StatusType.GET);
    KVMessage clientKvMessageKey2Get =
        new KVMessage(key2, null, clientId, KVMessage.StatusType.GET);
    KVMessage clientKvMessageKey3Get =
        new KVMessage(key3, null, clientId, KVMessage.StatusType.GET);

    clientKvMessageKey1.calculateMAC();
    clientKvMessageKey2.calculateMAC();
    clientKvMessageKey3.calculateMAC();
    skvmngr.handleClientRequest(clientKvMessageKey1);
    skvmngr.handleClientRequest(clientKvMessageKey2);
    skvmngr.handleClientRequest(clientKvMessageKey3);

    clientKvMessageKey1Get.calculateMAC();
    clientKvMessageKey2Get.calculateMAC();
    clientKvMessageKey3Get.calculateMAC();

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
    ECSMessage message = new ECSMessage(ECSMessage.ActionType.MOVE_DATA, ECSMetadata.getInstance());
    message.calculateAndSetMAC();

    DataTransferMessage dtmsg = skvmngr.partitionDatabaseAndGetKeysInRange(message, hashRange);

    // key1 should not be on disk anymore, key2 and key3 should be
    assertEquals(
        KVMessage.StatusType.GET_ERROR,
        skvmngr.handleClientRequest(clientKvMessageKey1Get).getStatus());
    assertEquals(
        KVMessage.StatusType.GET_SUCCESS,
        skvmngr.handleClientRequest(clientKvMessageKey2Get).getStatus());
    assertEquals(
        KVMessage.StatusType.GET_SUCCESS,
        skvmngr.handleClientRequest(clientKvMessageKey3Get).getStatus());

    // key1 should be part of the payload, key2 and key3 should not
    assertFalse(dtmsg.getPayload().isEmpty());
    assertEquals(((StorageUnit) (dtmsg.getPayload().toArray()[0])).key, key1);
    assertNotEquals(((StorageUnit) (dtmsg.getPayload().toArray()[0])).key, key2);
    assertNotEquals(((StorageUnit) (dtmsg.getPayload().toArray()[0])).key, key3);
  }

  private void assertNotEquals(String key, String key2) {
    assertFalse(key.equals(key2));
  }
}
