package testing;

import static shared.communication.messages.DataTransferMessage.DataTransferMessageType.DATA_TRANSFER_REQUEST;

import app_kvServer.data.SynchronizedKVManager;
import java.util.HashMap;
import junit.framework.TestCase;
import org.apache.commons.lang3.RandomStringUtils;
import shared.communication.messages.DataTransferMessage;
import shared.communication.messages.KVMessage;

public class SynchronizedKVManagerTest extends TestCase {
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
    KVMessage kvMessagePut = new KVMessage(keyPut, "testing123", KVMessage.StatusType.PUT);
    KVMessage kvMessagePutGet = new KVMessage(keyPut, null, KVMessage.StatusType.GET);
    KVMessage kvMessageKey1 = new KVMessage(key1, null, KVMessage.StatusType.GET);
    KVMessage kvMessageKey2 = new KVMessage(key2, null, KVMessage.StatusType.GET);

    skvmngr.handleRequest(kvMessagePut);
    assertEquals("testing123", skvmngr.handleRequest(kvMessagePutGet).getValue());

    skvmngr.handleDataTransfer(dtmsg);
    assertEquals("abcde", skvmngr.handleRequest(kvMessageKey1).getValue());
    assertEquals("qwerty", skvmngr.handleRequest(kvMessageKey2).getValue());
    assertEquals("testing123", skvmngr.handleRequest(kvMessagePutGet).getValue());
  }

  public void testPartitionDatabaseAndGetKeysInRange() {
    SynchronizedKVManager skvmngr = SynchronizedKVManager.getInstance();

    String key1 = "qwfgth"; // MD5 hash: 338d66d0d3b47cb2a94876d20024bd6e
    String key2 = "kjkljl"; // MD5 hash: b3ec1f1c725bb7e274fb59854cca6c9d
    String key3 = "jlkhh"; // MD5 hash: cfeb063f8604de1a1784b932a19d3c91

    KVMessage kvMessageKey1 = new KVMessage(key1, "abc", KVMessage.StatusType.PUT);
    KVMessage kvMessageKey2 = new KVMessage(key2, "def", KVMessage.StatusType.PUT);
    KVMessage kvMessageKey3 = new KVMessage(key3, "ghi", KVMessage.StatusType.PUT);

    KVMessage kvMessageKey1Get = new KVMessage(key1, null, KVMessage.StatusType.GET);
    KVMessage kvMessageKey2Get = new KVMessage(key2, null, KVMessage.StatusType.GET);
    KVMessage kvMessageKey3Get = new KVMessage(key3, null, KVMessage.StatusType.GET);

    skvmngr.handleRequest(kvMessageKey1);
    skvmngr.handleRequest(kvMessageKey2);
    skvmngr.handleRequest(kvMessageKey3);

    // Ensure all keys on disk
    assertEquals("abc", skvmngr.handleRequest(kvMessageKey1Get).getValue());
    assertEquals("def", skvmngr.handleRequest(kvMessageKey2Get).getValue());
    assertEquals("ghi", skvmngr.handleRequest(kvMessageKey3Get).getValue());

    // partition by Key1 hash upto Key2 hash (should be not included in partition)
    String[] hashRange =
        new String[] {
          "338d66d0d3b47cb2a94876d20024bd6e".toUpperCase(),
          "b3ec1f1c725bb7e274fb59854cca6c9d".toUpperCase()
        };
    DataTransferMessage dtmsg = skvmngr.partitionDatabaseAndGetKeysInRange(hashRange);

    // key1 should not be on disk anymore, key2 and key3 should be
    assertEquals(
        KVMessage.StatusType.GET_ERROR, skvmngr.handleRequest(kvMessageKey1Get).getStatus());
    assertEquals(
        KVMessage.StatusType.GET_SUCCESS, skvmngr.handleRequest(kvMessageKey2Get).getStatus());
    assertEquals(
        KVMessage.StatusType.GET_SUCCESS, skvmngr.handleRequest(kvMessageKey3Get).getStatus());

    // key1 should be part of the payload, key2 and key3 should not
    assertTrue(dtmsg.getPayload().containsKey(key1));
    assertFalse(dtmsg.getPayload().containsKey(key2));
    assertFalse(dtmsg.getPayload().containsKey(key3));
  }
}
