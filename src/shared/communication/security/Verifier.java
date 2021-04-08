package shared.communication.security;

import java.security.PublicKey;
import shared.communication.messages.ECSMessage;
import shared.communication.messages.KVMessage;
import shared.communication.security.encryption.AsymmetricEncryption;
import shared.communication.security.encryption.AsymmetricEncryptionException;
import shared.communication.security.property_stores.ServerPropertyStore;

public class Verifier {
  public static boolean verifyKVMessageMAC(KVMessage message) throws AsymmetricEncryptionException {
    PublicKey publicKey = ServerPropertyStore.getInstance().getClientPublicKey();
    if (publicKey == null) {
      throw new NullPointerException("Client Public key not initialized");
    }
    if (message.getMAC() == null) return false;
    String decryptedHash = AsymmetricEncryption.decryptString(message.getMAC(), publicKey);
    String calculatedHash = message.generateMessageHash();
    return decryptedHash.equals(calculatedHash);
  }

  public static boolean verifyKVCheck(String key, String value, String kvCheck)
      throws AsymmetricEncryptionException {
    PublicKey publicKey = ServerPropertyStore.getInstance().getClientPublicKey();
    if (publicKey == null) {
      throw new NullPointerException("Client Public key not initialized");
    }
    String decryptedHash = AsymmetricEncryption.decryptString(kvCheck, publicKey);
    String calculatedHash = KVMessage.generateKVHash(key, value);
    return decryptedHash.equals(calculatedHash);
  }

  public static boolean verifyECSMessageMAC(ECSMessage message)
      throws AsymmetricEncryptionException {
    PublicKey publicKey = ServerPropertyStore.getInstance().getECSPublicKey();
    if (publicKey == null) {
      throw new NullPointerException("ECS Public key not initialized");
    }
    if (message.getMAC() == null) return false;
    return AsymmetricEncryption.decryptString(message.getMAC(), publicKey)
        .equals(message.generateHash());
  }
}
