package shared.communication.messages;

import ecs.ECSMetadata;
import org.apache.log4j.Logger;

public class KVMessage extends Message {
  private static final Logger logger = Logger.getLogger(KVMessage.class);
  private final String key;
  private final StatusType status_type;
  private final String value;
  // cannot be initialized other than through constructor, and are null if not initialized.
  private String errorMessage;
  private ECSMetadata metadata;

  // basic message constructor for both client and server
  public KVMessage(String key, String value, StatusType status_type) {
    this.key = key;
    this.value = value;
    this.status_type = status_type;
  }

  // error message constructor for server
  public KVMessage(String key, String value, StatusType status_type, String errorMessage) {
    this.key = key;
    this.value = value;
    this.status_type = status_type;
    this.errorMessage = errorMessage;
  }

  // error message constructor with metadata: used only by server in case of wrong target server
  public KVMessage(
      String key,
      String value,
      StatusType status_type,
      String errorMessage,
      ECSMetadata additionalData) {
    this.key = key;
    this.value = value;
    this.status_type = status_type;
    this.errorMessage = errorMessage;
    assert (this.status_type == StatusType.NOT_RESPONSIBLE);
    this.metadata = additionalData;
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
    SERVER_STOPPED
  }
}
