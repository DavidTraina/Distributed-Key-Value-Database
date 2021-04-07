package client;

public class ByzantineException extends Exception {
  public ByzantineException(String message) {
    super("Byzantine error: " + message);
  }
}
