package shared.communication.messages;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import ecs.ECSMetadata;
import java.nio.charset.StandardCharsets;
import org.apache.log4j.Logger;

public class KVMessage {
  private static final Logger logger = Logger.getLogger(KVMessage.class);
  private final String key;
  private final StatusType status_type;
  // cannot be initialized other than through constructor, and are null if not initialized.
  private String value;
  private String errorMessage;
  private ECSMetadata metadata;

  // basic message constructor for both client and server
  public KVMessage(String key, String value, StatusType status_type) {
    this.key = key;
    this.value = value;
    this.status_type = status_type;
  }

  // error message constructor for server
  public KVMessage(String key, StatusType status_type, String errorMessage) {
    this.key = key;
    this.status_type = status_type;
    this.errorMessage = errorMessage;
  }

  // error message constructor with metadata: used only by server in case of wrong target server
  public KVMessage(
      String key, StatusType status_type, String errorMessage, ECSMetadata additionalData) {
    this.key = key;
    this.status_type = status_type;
    this.errorMessage = errorMessage;
    assert (this.status_type == StatusType.NOT_RESPONSIBLE);
    this.metadata = additionalData;
  }

  public static KVMessage deserialize(byte[] bytes) throws KVMessageException {
    String messageJson = new String(bytes, StandardCharsets.UTF_8).trim();

    try {
      return new Gson().fromJson(messageJson, KVMessage.class);
    } catch (JsonSyntaxException e) {
      logger.error("Message JSON is invalid! \n" + messageJson);
      throw new KVMessageException("deserialization failed for " + messageJson);
    }
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

  /** @return a byte array ready for transporting over the network. */
  public byte[] serialize() {

    String messageJson = new Gson().toJson(this);
    byte[] bytes = messageJson.getBytes(StandardCharsets.UTF_8);
    return bytes;
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
