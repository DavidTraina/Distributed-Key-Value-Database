package shared.communication.messages;

import java.util.UUID;

public class ClientIdentificationMessage extends Message {
  private final UUID clientId;

  public ClientIdentificationMessage(UUID clientId) {
    this.clientId = clientId;
  }

  public UUID getClientId() {
    return clientId;
  }
}
