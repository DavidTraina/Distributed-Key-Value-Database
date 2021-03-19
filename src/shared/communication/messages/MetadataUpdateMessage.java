package shared.communication.messages;

import ecs.ECSMetadata;
import java.util.UUID;

public class MetadataUpdateMessage extends ClientServerMessage {
  final ECSMetadata metadata;

  // Response to request (non-null requestID) or new metadata (null requestID)
  public MetadataUpdateMessage(final ECSMetadata metadata, final UUID requestID) {
    super(requestID);
    assert (requestID != null);
    this.metadata = metadata;
  }

  // Request
  public MetadataUpdateMessage() {
    this.metadata = null;
  }

  public ECSMetadata getMetadata() {
    return metadata;
  }

  @Override
  public String toString() {
    return "MetadataUpdateMessage( reqId=" + getRequestId() + ", metadata=" + metadata + " )";
  }
}
