package shared.communication.messages;

import java.util.UUID;

public class ClientServerMessage extends Message {
  private final UUID requestId;

  public ClientServerMessage(final UUID requestId) {
    this.requestId = requestId;
  }

  public ClientServerMessage() {
    this.requestId = UUID.randomUUID();
  }

  /** @return request id message is associated with */
  public UUID getRequestId() {
    return requestId;
  }

  @Override
  public String toString() {
    return this.getClass() + ": requestId=" + requestId;
  }
}
