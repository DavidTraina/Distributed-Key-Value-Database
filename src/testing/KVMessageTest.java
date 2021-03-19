package testing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import org.junit.Test;
import shared.communication.messages.KVMessage;
import shared.communication.messages.Message;
import shared.communication.messages.MessageException;

public class KVMessageTest {
  String KEY = "mockKey";
  String VALUE = "mockValue";
  KVMessage.StatusType STATUS = KVMessage.StatusType.PUT;

  @Test
  public void testConstruct() {
    KVMessage message = new KVMessage(KEY, VALUE, STATUS);

    assertNotNull(message);
  }

  @Test
  public void testGetMembers() {
    KVMessage message = new KVMessage(KEY, VALUE, STATUS);

    assertEquals(KEY, message.getKey());
    assertEquals(VALUE, message.getValue());
    assertEquals(STATUS, message.getStatus());
  }

  @Test
  public void testSerializeAndDeserialize() {
    KVMessage message = new KVMessage(KEY, VALUE, STATUS);
    byte[] byteArray = message.serialize();
    assertNotNull(byteArray);
    try {
      KVMessage returnMessage = (KVMessage) Message.deserialize(byteArray);
      assertEquals(message.getKey(), returnMessage.getKey());
      assertEquals(message.getValue(), returnMessage.getValue());
      assertEquals(message.getStatus(), returnMessage.getStatus());
    } catch (MessageException e) {
      System.out.println("Exception on deserialization occurred.");
    }
  }

  @Test
  public void testDeserializeWrongByteArray() {
    Exception ex = null;
    byte[] randomByteArray = "asdfghjkl".getBytes(StandardCharsets.UTF_8);
    try {
      Message.deserialize(randomByteArray);
    } catch (Exception e) {
      ex = e;
    }
    assertTrue(ex instanceof MessageException);
  }
}
