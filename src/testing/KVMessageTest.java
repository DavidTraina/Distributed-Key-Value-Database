package testing;

import static org.junit.Assert.*;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.junit.Test;
import shared.communication.messages.KVMessage;
import shared.communication.messages.Message;
import shared.communication.messages.MessageException;
import shared.communication.security.KeyGenerator;
import shared.communication.security.property_stores.ClientPropertyStore;

public class KVMessageTest {
  String KEY = "mockKey";
  String VALUE = "mockValue";
  KVMessage.StatusType STATUS = KVMessage.StatusType.PUT;
  final UUID CLIENT_ID = UUID.randomUUID();

  @Test
  public void testConstruct() {
    KVMessage message = new KVMessage(KEY, VALUE, CLIENT_ID, STATUS);

    assertNotNull(message);
  }

  @Test
  public void testGetMembers() {
    KVMessage message = new KVMessage(KEY, VALUE, CLIENT_ID, STATUS);

    assertEquals(KEY, message.getKey());
    assertEquals(VALUE, message.getValue());
    assertEquals(STATUS, message.getStatus());
    assertEquals(CLIENT_ID, message.getClientId());
    assertNull(message.getMAC());
  }

  @Test
  public void testGetMembersAfterCalculations() {
    KeyGenerator generator = new KeyGenerator(1024);
    generator.createKeys();
    ClientPropertyStore.getInstance().setPrivateKey(generator.getPrivateKey());
    ClientPropertyStore.getInstance().setSenderID("test");
    KVMessage message = new KVMessage(KEY, VALUE, CLIENT_ID, STATUS);
    message.calculateMAC();

    assertEquals(KEY, message.getKey());
    assertEquals(VALUE, message.getValue());
    assertEquals(STATUS, message.getStatus());
    assertEquals(CLIENT_ID, message.getClientId());
    assertNotNull(message.getMAC());
    System.out.println(message.getSenderID());
    assertNotNull(message.getSenderID());
  }

  @Test
  public void testSerializeAndDeserialize() {
    KVMessage message = new KVMessage(KEY, VALUE, CLIENT_ID, STATUS);
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
