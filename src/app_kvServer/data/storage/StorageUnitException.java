package app_kvServer.data.storage;

public class StorageUnitException extends Exception {
  public StorageUnitException(String message) {
    super("StorageUnit error: " + message);
  }
}
