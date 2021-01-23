package shared.messages;

import com.google.gson.Gson;

public class KVMessage {

  private final String key;
  private final String value;
  private final StatusType status_type;

  final char LINE_FEED = 0x0A;
  final char RETURN = 0x0D;

  public KVMessage(String key, String value, StatusType status_type) {
    this.key = key;
    this.value = value;
    this.status_type = status_type;
  }

  public KVMessage(byte[] bytes) {
    KVMessage message = this.deserialize(bytes);
    this.key = message.getKey();
    this.value = message.getValue();
    this.status_type = message.getStatus();
  }

  /** @return the key that is associated with this message, null if not key is associated. */
  public String getKey(){
    return key;
  }

  /** @return the value that is associated with this message, null if not value is associated. */
  public String getValue(){
    return value;
  }

  /**
   * @return a status string that is used to identify request types, response types and error types
   *     associated to the message.
   */
  public StatusType getStatus(){
    return status_type;
  }

  public byte[] serialize(){

    String messageJson = new Gson().toJson(this);

    byte[] bytes = messageJson.getBytes();
    byte[] ctrBytes = new byte[] {LINE_FEED, RETURN};
    byte[] messageBytes = new byte[bytes.length + ctrBytes.length];

    System.arraycopy(bytes, 0, messageBytes, 0, bytes.length);
    System.arraycopy(ctrBytes, 0, messageBytes, bytes.length, ctrBytes.length);

    return messageBytes;
  }

  public KVMessage deserialize(byte[] bytes){
    byte[] ctrBytes = new byte[] {LINE_FEED, RETURN};
    byte[] messageBytes = new byte[bytes.length + ctrBytes.length];

    System.arraycopy(bytes, 0, messageBytes, 0, bytes.length);
    System.arraycopy(ctrBytes, 0, messageBytes, bytes.length, ctrBytes.length);

    String messageJson = new String(messageBytes).trim();
    Gson gson = new Gson();
    KVMessage message = gson.fromJson(messageJson, KVMessage.class);
    return message;
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
    DELETE_ERROR, /* Delete - request successful */
  }
}
