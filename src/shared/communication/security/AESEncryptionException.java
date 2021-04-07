package shared.communication.security;

public class AESEncryptionException extends Exception {
  public AESEncryptionException(String message) {
    super("AESEncryption error: " + message);
  }
}
