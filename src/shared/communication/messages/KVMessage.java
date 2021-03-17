package shared.communication.messages;

public class KVMessage extends Message {
  private final String key;
  private final StatusType status_type;
  private final String value;

  // basic message constructor for both client and server
  public KVMessage(String key, String value, StatusType status_type) {
    this.key = key;
    this.value = value;
    this.status_type = status_type;
  }

  /** @return the key that is associated with this message, null if not key is associated. */
  public String getKey() {
    return key;
  }

  /** @return the value that is associated with this message, null if not value is associated. */
  public String getValue() {
    return value;
  }

  /**
   * @return a status string that is used to identify request types, response types and error types
   *     associated to the message.
   */
  public StatusType getStatus() {
    return status_type;
  }
}
