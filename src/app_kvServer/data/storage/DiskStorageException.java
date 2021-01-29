package app_kvServer.data.storage;

public class DiskStorageException extends Exception {
  public DiskStorageException(String message) {
    super("Disk storage error: " + message);
  }
}
