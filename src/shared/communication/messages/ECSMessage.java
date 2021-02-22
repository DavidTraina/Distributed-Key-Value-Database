package shared.communication.messages;

import ecs.ECSMetadata;

public class ECSMessage extends Message {
  private final ActionType action;
  private ECSMetadata metadata;
  private String dataTransferServer;
  private String[] dataTransferHashRange;

  public ECSMessage(ActionType action, ECSMetadata metadata) {
    this.action = action;
    this.metadata = metadata;
  }

  public ECSMessage(ActionType action) {
    this.action = action;
  }

  public ECSMessage(ActionType action, String server, String[] hashRange) {
    this.action = action;
    this.dataTransferServer = server;
    this.dataTransferHashRange = hashRange;
  }

  public ActionType getAction() {
    return action;
  }

  public ECSMetadata getMetadata() {
    return metadata;
  }

  public String getDataTransferServer() {
    return dataTransferServer;
  }

  public String[] getDataTransferHashRange() {
    return dataTransferHashRange;
  }

  enum ActionType {
    INIT,
    START,
    STOP,
    SHUTDOWN,
    LOCK_WRITE,
    UNLOCK_WRITE,
    MOVE_DATA,
    UPDATE_METADATA
  }
}
