package shared.messages;

public class KVMessageException extends Exception {
  public KVMessageException(String message) {
    super("KVMessage error: " + message);
  }
}
