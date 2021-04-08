package shared.communication.security.encryption;

public class AESEncryptionException extends Exception {
  public AESEncryptionException(String message) {
    super("AESEncryption error: " + message);
  }
}
