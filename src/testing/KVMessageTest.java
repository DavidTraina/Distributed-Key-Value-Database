package testing;

import java.nio.charset.StandardCharsets;
import junit.framework.TestCase;
import shared.communication.messages.KVMessage;
import shared.communication.messages.KVMessageException;

public class KVMessageTest extends TestCase {
  String KEY = "mockKey";
  String VALUE = "mockValue";
  KVMessage.StatusType STATUS = KVMessage.StatusType.PUT;

  public void testConstruct() {
    KVMessage message = new KVMessage(KEY, VALUE, STATUS);

    assertNotNull(message);
  }

  public void testGetMembers() {
    KVMessage message = new KVMessage(KEY, VALUE, STATUS);

    assertEquals(KEY, message.getKey());
    assertEquals(VALUE, message.getValue());
    assertEquals(STATUS, message.getStatus());
  }

  public void testSerializeAndDeserialize() {
    KVMessage message = new KVMessage(KEY, VALUE, STATUS);
    byte[] byteArray = message.serialize();
    assertNotNull(byteArray);
    try {
      KVMessage returnMessage = KVMessage.deserialize(byteArray);
      assertEquals(message.getKey(), returnMessage.getKey());
      assertEquals(message.getValue(), returnMessage.getValue());
      assertEquals(message.getStatus(), returnMessage.getStatus());
    } catch (KVMessageException e) {
      System.out.println("Exception on deserialization occurred.");
    }
  }

  public void testDeserializeWrongByteArray() {
    Exception ex = null;
    byte[] randomByteArray = "asdfghjkl".getBytes(StandardCharsets.UTF_8);
    try {
      KVMessage.deserialize(randomByteArray);
    } catch (Exception e) {
      ex = e;
    }
    assertTrue(ex instanceof KVMessageException);
  }
}
