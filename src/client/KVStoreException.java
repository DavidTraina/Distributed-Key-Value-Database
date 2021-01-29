package client;

public class KVStoreException extends Exception {
  public KVStoreException(String message) {
    super("KVStore error: " + message);
  }
}
