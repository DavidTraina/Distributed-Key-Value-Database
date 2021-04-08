package shared.communication.security.property_stores;

import java.security.PublicKey;

/** Class that stores properties that can only be initialized one-time */
public enum ServerPropertyStore {
  INSTANCE;
  private String senderID = null;

  // used by servers only
  private PublicKey clientPublicKey = null;
  private PublicKey ecsPublicKey = null;

  ServerPropertyStore() {}

  public void setSenderID(final String senderID) {
    if (this.senderID == null) this.senderID = senderID;
  }

  public static ServerPropertyStore getInstance() {
    return INSTANCE;
  }

  public String getSenderID() {
    return this.senderID;
  }

  public PublicKey getClientPublicKey() {
    return clientPublicKey;
  }

  public void setClientPublicKey(PublicKey clientPublicKey) {
    if (this.clientPublicKey == null) this.clientPublicKey = clientPublicKey;
  }

  public PublicKey getECSPublicKey() {
    return ecsPublicKey;
  }

  public void setECSPublicKey(PublicKey ecsPublicKey) {
    if (this.ecsPublicKey == null) this.ecsPublicKey = ecsPublicKey;
  }
}
