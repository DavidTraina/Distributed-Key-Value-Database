package shared.exceptions;

public class ConnectionLostException extends Exception {
  public ConnectionLostException(String message) {
    super(message);
  }
}
