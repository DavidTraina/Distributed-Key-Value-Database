package shared.communication.security;

import app_kvServer.data.storage.StorageUnit;
import java.security.PublicKey;
import shared.communication.messages.ECSMessage;
import shared.communication.messages.KVMessage;
import shared.communication.security.encryption.Encryption;
import shared.communication.security.encryption.EncryptionException;
import shared.communication.security.property_stores.ServerPropertyStore;

public class Verifier {
  public static boolean verifyKVMessageMAC(KVMessage message) throws EncryptionException {
    PublicKey publicKey = ServerPropertyStore.getInstance().getClientPublicKey();
    if (publicKey == null) {
      throw new NullPointerException("Client Public key not initialized");
    }
    if (message.getMAC() == null) return false;
    String decryptedHash =
        Encryption.decryptString(message.getMAC(), publicKey, Encryption.EncryptionType.RSA);
    String calculatedHash = message.generateMessageHash();
    return decryptedHash.equals(calculatedHash);
  }

  public static boolean verifyStorageUnitMAC(StorageUnit unit) throws EncryptionException {
    PublicKey publicKey = ServerPropertyStore.getInstance().getClientPublicKey();
    if (publicKey == null) {
      throw new NullPointerException("Client Public key not initialized");
    }
    if (unit.MAC == null) return false;
    String decryptedHash =
        Encryption.decryptString(unit.MAC, publicKey, Encryption.EncryptionType.RSA);
    String calculatedHash =
        KVMessage.generateMessageHashFromOutside(unit.key, unit.value, unit.uniqueID);
    return decryptedHash.equals(calculatedHash);
  }

  public static boolean verifyECSMessageMAC(ECSMessage message) throws EncryptionException {
    PublicKey publicKey = ServerPropertyStore.getInstance().getECSPublicKey();
    if (publicKey == null) {
      throw new NullPointerException("ECS Public key not initialized");
    }
    if (message.getMAC() == null) return false;
    return Encryption.decryptString(message.getMAC(), publicKey, Encryption.EncryptionType.RSA)
        .equals(message.generateHash());
  }
}
