package shared.communication.security;

/** Class that stores properties that can only be initialized one-time */
public enum PropertyStore {
  INSTANCE;
  private String senderID = null;

  PropertyStore() {}

  public void setSenderID(final String senderID) {
    if (this.senderID == null) this.senderID = senderID;
  }

  public static PropertyStore getInstance() {
    return INSTANCE;
  }

  public String getSenderID() {
    return this.senderID;
  }
}
