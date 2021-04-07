package shared.communication.messages;

public class ReplicationMessage extends Message {
  private final KVMessage message;

  public ReplicationMessage(final KVMessage message) {
    this.message = message;
  }

  public KVMessage getMessage() {
    return message;
  }
}
