package shared.communication.messages;

import java.security.PrivateKey;
import java.util.UUID;
import shared.communication.security.Hashing;
import shared.communication.security.encryption.Encryption;
import shared.communication.security.encryption.EncryptionException;
import shared.communication.security.property_stores.ClientPropertyStore;

public class KVMessage extends ClientServerMessage {
  private final String key;
  private final String value;

  private final StatusType statusType;
  private final String senderID = ClientPropertyStore.getInstance().getSenderID();
  private final String timestamp = String.valueOf(System.currentTimeMillis());
  private final UUID clientId;

  private String MAC = null;
  // basic request message constructor for client
  public KVMessage(String key, String value, UUID clientId, StatusType statusType) {
    this.key = key;
    this.value = value;
    this.clientId = clientId;
    this.statusType = statusType;
  }

  // basic response message constructor for server
  public KVMessage(String key, String value, UUID clientId, StatusType statusType, UUID requestId) {
    super(requestId);
    this.key = key;
    this.value = value;
    this.clientId = clientId;
    this.statusType = statusType;
  }

  public UUID getClientId() {
    return clientId;
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

  /** @return a string that is used to uniquely identify the sender of the message */
  public String getSenderID() {
    return senderID;
  }

  public String getMAC() {
    return MAC;
  }

  public String getUniqueID() {
    return timestamp + senderID;
  }

  public String generateMessageHash() {
    return Hashing.calculateMD5Hash(this.key + this.value + this.timestamp + this.senderID);
  }

  public static String generateMessageHashFromOutside(
      String key, String value, String timestampAndSenderId) {
    return Hashing.calculateMD5Hash(key + value + timestampAndSenderId);
  }

  public static String generateKVHash(String key, String value) {
    return Hashing.calculateMD5Hash(key + value);
  }

  // only used by the client
  public KVMessage calculateMAC() {
    try {
      PrivateKey privateKey = ClientPropertyStore.getInstance().getPrivateKey();
      if (privateKey == null) {
        throw new NullPointerException("Private key not initialized");
      }
      this.MAC =
          Encryption.encryptString(
              this.generateMessageHash(), privateKey, Encryption.EncryptionType.RSA);
    } catch (EncryptionException e) {
      e.printStackTrace();
    }
    return this;
  }

  @Override
  public String toString() {
    return "KVMessage{"
        + "key='"
        + key
        + "', val='"
        + value
        + "', client='"
        + clientId
        + "', status='"
        + statusType
        + "', sender='"
        + senderID
        + ", timestamp='"
        + timestamp
        + "', MAC='"
        + MAC
        + "', reqId='"
        + getRequestId()
        + "'}";
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
    SUBSCRIBE,
    UNSUBSCRIBE,
    SUBSCRIBE_SUCCESS,
    SUBSCRIBE_ERROR,
    UNSUBSCRIBE_SUCCESS,
    UNSUBSCRIBE_ERROR,
    NOTIFY,
    FAILED,
    AUTH_FAILED,

    NOT_RESPONSIBLE,
    SERVER_WRITE_LOCK,
    SERVER_STOPPED,
  }
}
