package testing;

import static org.junit.Assert.*;

import app_kvServer.data.storage.StorageUnit;
import java.util.UUID;
import org.junit.BeforeClass;
import org.junit.Test;
import shared.communication.messages.ECSMessage;
import shared.communication.messages.KVMessage;
import shared.communication.security.KeyGenerator;
import shared.communication.security.Verifier;
import shared.communication.security.encryption.EncryptionException;
import shared.communication.security.property_stores.ClientPropertyStore;
import shared.communication.security.property_stores.ECSPropertyStore;
import shared.communication.security.property_stores.ServerPropertyStore;

public class VerifierTest {
  String KEY = "mockKey";
  String VALUE = "mockValue";
  KVMessage.StatusType STATUS = KVMessage.StatusType.PUT;

  @BeforeClass
  public static void setKeys() {
    KeyGenerator generator = new KeyGenerator(1024);
    generator.createKeys();
    ClientPropertyStore.getInstance().setPrivateKey(generator.getPrivateKey());
    ServerPropertyStore.getInstance().setClientPublicKey(generator.getPublicKey());

    generator.createKeys();
    ECSPropertyStore.getInstance().setPrivateKey(generator.getPrivateKey());
    ServerPropertyStore.getInstance().setECSPublicKey(generator.getPublicKey());
  }

  @Test
  public void testVerifyKVMessageMACFailsOnInvalidKeys() {
    KVMessage message = new KVMessage(KEY, VALUE, UUID.randomUUID(), STATUS);
    try {
      assertFalse(Verifier.verifyKVMessageMAC(message));
    } catch (EncryptionException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void verifyStorageUnitMACAndFail() {
    KVMessage message = new KVMessage(KEY, VALUE, UUID.randomUUID(), STATUS);
    StorageUnit storageUnit = new StorageUnit(KEY, VALUE, message.getUniqueID(), message.getMAC());
    try {
      assertFalse(Verifier.verifyStorageUnitMAC(storageUnit));
    } catch (EncryptionException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testVerifyECSMessageMACFails() {
    ECSMessage message = new ECSMessage(ECSMessage.ActionType.INIT);
    try {
      assertFalse(Verifier.verifyECSMessageMAC(message));
    } catch (EncryptionException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testVerifyKVMessageMAC() {
    KVMessage message = new KVMessage(KEY, VALUE, UUID.randomUUID(), STATUS);
    message.calculateMAC();
    try {
      assertTrue(Verifier.verifyKVMessageMAC(message));
    } catch (EncryptionException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void verifyStorageUnitMAC() {
    KVMessage message = new KVMessage(KEY, VALUE, UUID.randomUUID(), STATUS);
    message.calculateMAC();
    StorageUnit storageUnit = new StorageUnit(KEY, VALUE, message.getUniqueID(), message.getMAC());
    try {
      assertTrue(Verifier.verifyStorageUnitMAC(storageUnit));
    } catch (EncryptionException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testVerifyECSMessageMAC() {
    ECSMessage message = new ECSMessage(ECSMessage.ActionType.INIT).calculateAndSetMAC();
    try {
      assertTrue(Verifier.verifyECSMessageMAC(message));
    } catch (EncryptionException e) {
      e.printStackTrace();
      fail();
    }
  }
}
