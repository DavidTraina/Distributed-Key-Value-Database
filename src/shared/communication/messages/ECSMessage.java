package shared.communication.messages;

import ecs.ECSMetadata;
import ecs.ECSNode;
import java.security.PrivateKey;
import java.util.Arrays;
import shared.communication.security.*;
import shared.communication.security.encryption.Encryption;
import shared.communication.security.encryption.EncryptionException;
import shared.communication.security.property_stores.ECSPropertyStore;

public class ECSMessage extends Message {
  private final ActionType action;
  private ActionStatus status;
  private ECSMetadata metadata;
  private ECSNode dataTransferServer;
  private String[] dataTransferHashRange;
  private String message;
  private final String senderID = ECSPropertyStore.getInstance().getSenderID();
  private final String timestamp = String.valueOf(System.currentTimeMillis());
  public String MAC = null;

  public ECSMessage(ActionType action, ECSMetadata metadata) {
    this.action = action;
    this.metadata = metadata;
  }

  public ECSMessage(ActionType action) {
    this.action = action;
  }

  public ECSMessage(ActionType action, ECSNode server, String[] hashRange) {
    this.action = action;
    this.dataTransferServer = server;
    this.dataTransferHashRange = hashRange;
  }

  public ECSMessage(ActionStatus status, String message) {
    this.action = null;
    this.status = status;
    this.message = message;
  }

  public ActionStatus getStatus() {
    return status;
  }

  public String getMessage() {
    return message;
  }

  public ActionType getAction() {
    return action;
  }

  public ECSMetadata getMetadata() {
    return metadata;
  }

  public ECSNode getDataTransferServer() {
    return dataTransferServer;
  }

  public String[] getDataTransferHashRange() {
    return dataTransferHashRange;
  }

  public String getMAC() {
    return MAC;
  }

  public String getSenderID() {
    return senderID;
  }

  public String generateHash() {
    return Hashing.calculateMD5Hash(this.timestamp + this.senderID);
  }

  // only used by the ecs
  public ECSMessage calculateAndSetMAC() {
    try {
      PrivateKey privateKey = ECSPropertyStore.getInstance().getPrivateKey();
      if (privateKey == null) {
        throw new NullPointerException("Private key not initialized");
      }
      this.MAC =
          Encryption.encryptString(this.generateHash(), privateKey, Encryption.EncryptionType.RSA);
    } catch (EncryptionException e) {
      e.printStackTrace();
    }
    return this;
  }

  @Override
  public String toString() {
    return "ECSMessage( action="
        + action
        + ", status="
        + status
        + ", message="
        + message
        + ", metadata="
        + metadata
        + ", dataTransferServer="
        + dataTransferServer
        + ", hashRange="
        + Arrays.toString(dataTransferHashRange)
        + ", senderID='"
        + senderID
        + " )";
  }

  public enum ActionType {
    INIT,
    START,
    STOP,
    SHUTDOWN,
    LOCK_WRITE,
    UNLOCK_WRITE,
    MOVE_DATA,
    UPDATE_METADATA
  }

  public enum ActionStatus {
    ACTION_SUCCESS,
    ACTION_FAILED
  }
}
