package testing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.nio.charset.StandardCharsets;
import org.junit.Test;
import shared.communication.messages.ClientKVMessage;
import shared.communication.messages.Message;
import shared.communication.messages.MessageException;

public class ClientKVMessageTest {
  String KEY = "mockKey";
  String VALUE = "mockValue";
  ClientKVMessage.StatusType STATUS = ClientKVMessage.StatusType.PUT;

  @Test
  public void testConstruct() {
    ClientKVMessage message = new ClientKVMessage(KEY, VALUE, STATUS);

    assertNotNull(message);
  }

  @Test
  public void testGetMembers() {
    ClientKVMessage message = new ClientKVMessage(KEY, VALUE, STATUS);

    assertEquals(KEY, message.getKey());
    assertEquals(VALUE, message.getValue());
    assertEquals(STATUS, message.getStatus());
  }

  @Test
  public void testSerializeAndDeserialize() {
    ClientKVMessage message = new ClientKVMessage(KEY, VALUE, STATUS);
    byte[] byteArray = message.serialize();
    assertNotNull(byteArray);
    try {
      ClientKVMessage returnMessage = (ClientKVMessage) Message.deserialize(byteArray);
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
