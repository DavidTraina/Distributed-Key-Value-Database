package shared.communication.messages;

import java.util.UUID;

public class KVMessage extends ClientServerMessage {
  private final String key;
  private final String value;
  private final StatusType statusType;

  // basic request message constructor for client
  public KVMessage(String key, String value, StatusType statusType) {
    this.key = key;
    this.value = value;
    this.statusType = statusType;
  }

  // basic response message constructor for server
  public KVMessage(String key, String value, StatusType statusType, UUID requestId) {
    super(requestId);
    this.key = key;
    this.value = value;
    this.statusType = statusType;
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
    return statusType;
  }

  @Override
  public String toString() {
    return "KVMessage( status="
        + statusType
        + ", key="
        + key
        + ", value="
        + value
        + ", reqId="
        + getRequestId()
        + " )";
  }

  public enum StatusType {
    GET, /* Get - request */
    GET_ERROR, /* requested tuple (i.e. value) not found */
    GET_SUCCESS, /* requested tuple (i.e. value) found */
    PUT, /* Put - request */
    PUT_SUCCESS, /* Put - request successful, tuple inserted */
    PUT_UPDATE, /* Put - request successful, i.e. value updated */
    PUT_ERROR, /* Put - request not successful */
    DELETE_SUCCESS, /* Delete - request successful */
    DELETE_ERROR, /* Delete - request not successful */
    FAILED,

    NOT_RESPONSIBLE,
    SERVER_WRITE_LOCK,
    SERVER_STOPPED,
  }
}
