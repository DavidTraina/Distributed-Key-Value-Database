package shared.communication.messages;

import app_kvServer.data.storage.DiskStorage;
import java.util.HashMap;

public class DataTransferMessage extends Message {
  private final DataTransferMessageType type;
  private HashMap<String, String> payload = new HashMap<>();
  private final String message;
  private String[] hashRange = null;

  public DiskStorage.StorageType getStorageType() {
    return storageType;
  }

  public void setStorageType(DiskStorage.StorageType storageType) {
    this.storageType = storageType;
  }

  private DiskStorage.StorageType storageType = DiskStorage.StorageType.SELF;

  public DataTransferMessage(
      final DataTransferMessageType type,
      final HashMap<String, String> payload,
      final String message) {
    this.type = type;
    this.payload = payload;
    this.message = message;
  }

  public DataTransferMessage(
      final DataTransferMessageType type, final String[] hashRange, final String message) {
    this.type = type;
    this.message = message;
    this.hashRange = hashRange;
  }

  public DataTransferMessage(final DataTransferMessageType type, final String message) {
    this.type = type;
    this.message = message;
  }

  public String[] getHashRange() {
    return this.hashRange;
  }

  public DataTransferMessageType getDataTransferMessageType() {
    return this.type;
  }

  public String getMessage() {
    return message;
  }

  public HashMap<String, String> getPayload() {
    return this.payload;
  }

  public enum DataTransferMessageType {
    DATA_TRANSFER_REQUEST,
    DATA_TRANSFER_SUCCESS,
    DATA_TRANSFER_FAILURE,
    MOVE_REPLICA2_TO_REPLICA1,
    MOVE_REPLICA1_TO_REPLICA2,
    DELETE_DATA
  }
}
