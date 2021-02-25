package shared.communication.messages;

import ecs.ECSMetadata;
import ecs.ECSNode;

public class ECSMessage extends Message {
  private final ActionType action;

  public ActionStatus getStatus() {
    return status;
  }

  public String getMessage() {
    return message;
  }

  private ActionStatus status;
  private ECSMetadata metadata;
  private ECSNode dataTransferServer;
  private String[] dataTransferHashRange;
  private String message;

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
