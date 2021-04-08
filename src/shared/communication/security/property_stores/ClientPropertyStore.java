package shared.communication.security.property_stores;

import java.security.PrivateKey;

/** Class that stores properties that can only be initialized one-time */
public enum ClientPropertyStore {
  INSTANCE;
  private String senderID = null;

  // used by Client
  private PrivateKey privateKey = null;

  ClientPropertyStore() {}

  public void setSenderID(final String senderID) {
    if (this.senderID == null) this.senderID = senderID;
  }

  public static ClientPropertyStore getInstance() {
    return INSTANCE;
  }

  public String getSenderID() {
    return this.senderID;
  }

  public PrivateKey getPrivateKey() {
    return privateKey;
  }

  public void setPrivateKey(PrivateKey privateKey) {
    if (this.privateKey == null) this.privateKey = privateKey;
  }
}
