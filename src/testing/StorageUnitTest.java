package testing;

import static org.junit.Assert.*;

import app_kvServer.data.storage.StorageUnit;
import app_kvServer.data.storage.StorageUnitException;
import javax.crypto.spec.SecretKeySpec;
import org.junit.Test;
import shared.communication.security.encryption.Encryption;

public class StorageUnitTest {
  String KEY = "mockKey";
  String VALUE = "mockValue";
  String KVCHECK = "mockCheck";
  String UNIQUEID = "mockCheck";

  @Test
  public void testConstruct() {
    StorageUnit unit = new StorageUnit(KEY, VALUE, KVCHECK, "aa");

    assertNotNull(unit);
  }

  @Test
  public void testGetMembers() {
    StorageUnit unit = new StorageUnit(KEY, VALUE, KVCHECK, UNIQUEID);

    assertEquals(KEY, unit.key);
    assertEquals(VALUE, unit.value);
  }

  @Test
  public void testSerializeAndDeserialize() {
    Exception exception = null;
    try {
      StorageUnit unit = new StorageUnit(KEY, VALUE, KVCHECK, UNIQUEID);
      String serializedUnit = unit.serialize(null);
      assertNotNull(serializedUnit);
      StorageUnit deserializedUnit = StorageUnit.deserialize(serializedUnit, null);
      assertEquals(KEY, unit.key);
      assertEquals(VALUE, unit.value);
    } catch (Exception e) {
      exception = e;
      System.out.println("Exception on deserialization occurred.");
    }
    assertNull(exception);
  }

  @Test
  public void testSerializeAndDeserializeWithEncryption() {
    Exception exception = null;
    try {
      SecretKeySpec aesEncryption = Encryption.createSecretKeySpec("abcde");
      StorageUnit unit = new StorageUnit(KEY, VALUE, KVCHECK, UNIQUEID);
      String serializedUnit = unit.serialize(aesEncryption);
      assertNotNull(serializedUnit);
      StorageUnit deserializedUnit = StorageUnit.deserialize(serializedUnit, aesEncryption);
      assertEquals(KEY, deserializedUnit.key);
      assertEquals(VALUE, deserializedUnit.value);
    } catch (Exception e) {
      exception = e;
      System.out.println(e.getLocalizedMessage());
      System.out.println("Exception on deserialization occurred.");
    }
    assertNull(exception);
  }

  @Test
  public void testDeserializeWrongString() {
    Exception ex = null;
    String randomString = "asdfghjkl";
    try {
      StorageUnit.deserialize(randomString, null);
    } catch (Exception e) {
      ex = e;
    }
    assertTrue(ex instanceof StorageUnitException);
  }
}
