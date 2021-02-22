package shared.communication.messages;

public class MessageException extends Exception {
  public MessageException(String message) {
    super("Message error: " + message);
  }
}
