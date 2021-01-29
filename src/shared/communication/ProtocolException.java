package shared.communication;

public class ProtocolException extends Exception {
  public ProtocolException(String message) {
    super("Protocol error: " + message);
  }
}
