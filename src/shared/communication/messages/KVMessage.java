package shared.communication.messages;

import java.security.PrivateKey;
import java.util.UUID;
import shared.communication.security.Hashing;
import shared.communication.security.encryption.AsymmetricEncryption;
import shared.communication.security.encryption.AsymmetricEncryptionException;
import shared.communication.security.property_stores.ClientPropertyStore;

public class KVMessage extends ClientServerMessage {
  private final String key;
  private final String value;
  private String KVCheck = null;

  private final StatusType statusType;
  private final String senderID = ClientPropertyStore.getInstance().getSenderID();
  private final String timestamp = String.valueOf(System.currentTimeMillis());

  private String MAC = null;

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

  public String getKVCheck() {
    return this.KVCheck;
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

  public String generateMessageHash() {
    return Hashing.calculateMD5Hash(this.key + this.value + this.timestamp + this.senderID);
  }

  public static String generateKVHash(String key, String value) {
    return Hashing.calculateMD5Hash(key + value);
  }

  // only used by the client
  public KVMessage calculateKVCheckAndMAC() {
    try {
      PrivateKey privateKey = ClientPropertyStore.getInstance().getPrivateKey();
      if (privateKey == null) {
        throw new NullPointerException("Private key not initialized");
      }
      this.MAC = AsymmetricEncryption.encryptString(this.generateMessageHash(), privateKey);
      this.KVCheck =
          AsymmetricEncryption.encryptString(
              KVMessage.generateKVHash(this.key, this.value), privateKey);
    } catch (AsymmetricEncryptionException e) {
      e.printStackTrace();
    }
    return this;
  }

  @Override
  public String toString() {
    return "KVMessage{"
        + "key='"
        + key
        + '\''
        + ", value='"
        + value
        + '\''
        + ", kvCheck='"
        + KVCheck
        + '\''
        + ", statusType="
        + statusType
        + ", senderID='"
        + senderID
        + '\''
        + ", timestamp='"
        + timestamp
        + '\''
        + ", MAC='"
        + MAC
        + '\''
        + '}';
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
    AUTH_FAILED,

    NOT_RESPONSIBLE,
    SERVER_WRITE_LOCK,
    SERVER_STOPPED,
  }
}
