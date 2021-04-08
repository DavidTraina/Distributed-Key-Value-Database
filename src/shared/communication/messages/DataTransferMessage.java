package shared.communication.messages;

import app_kvServer.data.storage.DiskStorage;
import app_kvServer.data.storage.StorageUnit;
import java.util.Arrays;
import java.util.HashSet;

public class DataTransferMessage extends Message {
  private final DataTransferMessageType type;
  private HashSet<StorageUnit> payload = new HashSet<>();
  private final String message;
  private String[] hashRange = null;
  private ECSMessage ecsMessage;

  public DiskStorage.StorageType getStorageType() {
    return storageType;
  }

  public void setStorageType(DiskStorage.StorageType storageType) {
    this.storageType = storageType;
  }

  private DiskStorage.StorageType storageType = DiskStorage.StorageType.SELF;

  public DataTransferMessage(
      final DataTransferMessageType type,
      final HashSet<StorageUnit> payload,
      final String message,
      ECSMessage ecsMessage) {
    this.type = type;
    this.payload = payload;
    this.message = message;
    this.ecsMessage = ecsMessage;
  }

  public DataTransferMessage(
      final DataTransferMessageType type,
      final String[] hashRange,
      final String message,
      ECSMessage ecsMessage) {
    this.type = type;
    this.message = message;
    this.hashRange = hashRange;
    this.ecsMessage = ecsMessage;
  }

  public DataTransferMessage(
      final DataTransferMessageType type, final String message, ECSMessage ecsMessage) {
    this.type = type;
    this.message = message;
    this.ecsMessage = ecsMessage;
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

  public ECSMessage getECSMessage() {
    return ecsMessage;
  }

  public HashSet<StorageUnit> getPayload() {
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

  @Override
  public String toString() {
    return "DataTransferMessage{"
        + "type="
        + type
        + ", message='"
        + message
        + '\''
        + ", hashRange="
        + Arrays.toString(hashRange)
        + ", storageType="
        + storageType
        + '}';
  }
}
