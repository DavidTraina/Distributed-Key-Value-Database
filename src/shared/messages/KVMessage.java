package shared.messages;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import java.nio.charset.StandardCharsets;
import org.apache.log4j.Logger;

public class KVMessage {
  private static final Logger logger = Logger.getLogger(KVMessage.class);
  private final String key;
  private final String value;
  private final StatusType status_type;

  public KVMessage(String key, String value, StatusType status_type) {
    this.key = key;
    this.value = value;
    this.status_type = status_type;
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
  }
}
