package testing;

import static org.junit.Assert.*;

import app_kvServer.data.storage.StorageUnit;
import app_kvServer.data.storage.StorageUnitException;
import org.junit.Test;
import shared.communication.security.encryption.AESEncryption;

public class StorageUnitTest {
  String KEY = "mockKey";
  String VALUE = "mockValue";
  String KVCHECK = "mockCheck";

  @Test
  public void testConstruct() {
    StorageUnit unit = new StorageUnit(KEY, VALUE, KVCHECK);

    assertNotNull(unit);
  }

  @Test
  public void testGetMembers() {
    StorageUnit unit = new StorageUnit(KEY, VALUE, KVCHECK);

    assertEquals(KEY, unit.key);
    assertEquals(VALUE, unit.value);
    assertEquals(StorageUnit.StorageType.KV, unit.storageType);
  }

  @Test
  public void testSerializeAndDeserialize() {
    Exception exception = null;
    try {
      StorageUnit unit = new StorageUnit(KEY, VALUE, KVCHECK);
      String serializedUnit = unit.serialize(null);
      assertNotNull(serializedUnit);
      StorageUnit deserializedUnit = StorageUnit.deserialize(serializedUnit, null);
      assertEquals(KEY, unit.key);
      assertEquals(VALUE, unit.value);
      assertEquals(StorageUnit.StorageType.KV, unit.storageType);
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
      AESEncryption aesEncryption = new AESEncryption("abcde");
      StorageUnit unit = new StorageUnit(KEY, VALUE, KVCHECK);
      String serializedUnit = unit.serialize(aesEncryption);
      assertNotNull(serializedUnit);
      StorageUnit deserializedUnit = StorageUnit.deserialize(serializedUnit, aesEncryption);
      assertEquals(KEY, deserializedUnit.key);
      assertEquals(VALUE, deserializedUnit.value);
      assertEquals(StorageUnit.StorageType.KV, deserializedUnit.storageType);
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
