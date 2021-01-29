package testing;

import client.KVStore;
import client.KVStoreException;
import java.net.InetAddress;
import java.util.Collections;
import junit.framework.TestCase;
import shared.communication.messages.KVMessage;
import shared.communication.messages.KVMessage.StatusType;

public class InteractionTest extends TestCase {

  private KVStore kvClient;

  public void setUp() {
    try {
      kvClient = new KVStore(InetAddress.getByName("localhost"), 50000);
      kvClient.connect();
    } catch (Exception e) {
      System.out.println("Exception on creation of KVClient.");
      e.printStackTrace();
    }
  }

  public void tearDown() {
    try {
      kvClient.disconnect();
    } catch (Exception e) {
      System.out.println("Could not properly tear down.");
      e.printStackTrace();
    }
  }

  public void testPut() {
    String key = "foo2";
    String value = "bar2";
    KVMessage response = null;
    Exception ex = null;

    try {
      response = kvClient.put(key, value);
    } catch (Exception e) {
      ex = e;
    }

    assertTrue(ex == null && response.getStatus() == StatusType.PUT_SUCCESS);
  }

  public void testPutDisconnected() {
    try {
      kvClient.disconnect();
    } catch (KVStoreException e) {
    }
    String key = "foo";
    String value = "bar";
    Exception ex = null;

    try {
      kvClient.put(key, value);
    } catch (Exception e) {
      ex = e;
    }

    assertTrue(ex instanceof KVStoreException);
    assertTrue(ex.getMessage().contains("Socket closed"));
  }

  public void testUpdate() {
    String key = "updateTestValue";
    String initialValue = "initial";
    String updatedValue = "updated";

    KVMessage response = null;
    Exception ex = null;

    try {
      kvClient.put(key, initialValue);
      response = kvClient.put(key, updatedValue);

    } catch (Exception e) {
      ex = e;
    }

    assertNull(ex);
    assertEquals(updatedValue, response.getValue());
    assertEquals(StatusType.PUT_UPDATE, response.getStatus());
  }

  public void testDelete() {
    String key = "deleteTestValue";
    String value = "toDelete";

    KVMessage response = null;
    Exception ex = null;

    try {
      kvClient.put(key, value);

      // key-value pair changed from <key, "null"> to <key, null> as per @41 on piazza
      response = kvClient.put(key, null);

    } catch (Exception e) {
      ex = e;
    }

    assertNull(ex);
    assertEquals(StatusType.DELETE_SUCCESS, response.getStatus());
  }

  public void testGet() {
    String key = "foo";
    String value = "bar";
    KVMessage response = null;
    Exception ex = null;

    try {
      kvClient.put(key, value);
      response = kvClient.get(key);
    } catch (Exception e) {
      ex = e;
    }

    assertNull(ex);
    assertEquals("bar", response.getValue());
    assertEquals(StatusType.GET_SUCCESS, response.getStatus());
  }

  public void testGetUnsetValue() {
    String key = "an unset value";
    KVMessage response = null;
    Exception ex = null;

    try {
      response = kvClient.get(key);
    } catch (Exception e) {
      ex = e;
    }

    assertNull(ex);
    assertEquals(StatusType.GET_ERROR, response.getStatus());
  }
  // --------------------------------------NEW TEST CASES----------------------------------------//

  public void testDeleteNonExistent() {
    String key = "deleteTestValue2";

    KVMessage response = null;
    Exception ex = null;

    try {
      // key-value pair changed from <key, "null"> to <key, null> as per @41 on piazza
      response = kvClient.put(key, null);

    } catch (Exception e) {
      ex = e;
    }

    assertNull(ex);
    assertEquals(StatusType.DELETE_ERROR, response.getStatus());
  }

  public void testGetMessageWithOversizedKey() {
    // US-ASCII chars are encoded as 1 bytes in UTF-8
    String key = String.join("", Collections.nCopies(20 + 1, "a"));
    KVMessage response = null;
    Exception ex = null;

    try {
      response = kvClient.get(key);
    } catch (Exception e) {
      ex = e;
    }

    assertNull(ex);
    assertEquals(StatusType.FAILED, response.getStatus());
    assertEquals("MessageTooLarge", response.getKey());
  }

  public void testPutMessageWithMaxSizeKeyAndValue() throws KVStoreException {
    // US-ASCII chars are encoded as 1 bytes in UTF-8
    String key = String.join("", Collections.nCopies(20, "a"));
    String value = String.join("", Collections.nCopies((120 * 1024), "a"));
    KVMessage response = null;
    Exception ex = null;

    try {
      response = kvClient.put(key, value);
    } catch (Exception e) {
      ex = e;
    }

    assertNull(ex);
    assertEquals(StatusType.PUT_SUCCESS, response.getStatus());
    assertEquals(key, response.getKey());
    assertEquals(value, response.getValue());
  }

  public void testPutMessageWithOversizedValue() {
    // US-ASCII chars are encoded as 1 bytes in UTF-8
    String key = "aNormalKey";
    String value = String.join("", Collections.nCopies((120 * 1024) + 1, "a"));
    KVMessage response = null;
    Exception ex = null;

    try {
      response = kvClient.put(key, value);
    } catch (Exception e) {
      ex = e;
    }

    assertNull(ex);
    assertEquals(StatusType.FAILED, response.getStatus());
    assertEquals("MessageTooLarge", response.getKey());
  }
}
