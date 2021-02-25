package shared.communication.messages;

import java.util.HashMap;

public class DataTransferMessage extends Message {
  private final DataTransferMessageType type;
  private final HashMap<String, String> payload;
  private final String message;

  public DataTransferMessage(
      final DataTransferMessageType type,
      final HashMap<String, String> payload,
      final String message) {
    this.type = type;
    this.payload = payload;
    this.message = message;
  }

  public DataTransferMessage(final DataTransferMessageType type, final String message) {
    this.type = type;
    this.message = message;
    this.payload = null;
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
    DATA_TRANSFER_FAILURE
  }
}
