package shared.communication.messages;

import ecs.ECSMetadata;

public class ClientKVMessage extends KVMessage {
  // cannot be initialized other than through constructor, and are null if not initialized.
  private String errorMessage;
  private ECSMetadata metadata;

  // basic message constructor for both client and server
  public ClientKVMessage(KVMessage kvMessage) {
    super(kvMessage.getKey(), kvMessage.getValue(), kvMessage.getStatus());
  }

  // basic message constructor for both client and server
  public ClientKVMessage(String key, String value, StatusType status_type) {
    super(key, value, status_type);
  }

  // error message constructor for server
  public ClientKVMessage(String key, String value, StatusType status_type, String errorMessage) {
    super(key, value, status_type);
    this.errorMessage = errorMessage;
  }

  // error message constructor with metadata: used only by server in case of wrong target server
  public ClientKVMessage(
      String key,
      String value,
      StatusType status_type,
      String errorMessage,
      ECSMetadata additionalData) {
    super(key, value, status_type);
    assert (status_type == StatusType.NOT_RESPONSIBLE);
    this.errorMessage = errorMessage;
    this.metadata = additionalData;
  }

  /**
   * @return the additional error info that is associated with this message, null if no additional
   *     error info is associated.
   */
  public String getErrorMessage() {
    return errorMessage;
  }

  /** @return metadata for all server nodes, null if no additional data is provided. */
  public ECSMetadata getMetadata() {
    return metadata;
  }
}
