package client;

public interface ClientSocketListener {

  public void handleNewMessage(TextMessage msg);

  public void handleStatus(SocketStatus status);

  public enum SocketStatus {
    CONNECTED,
    DISCONNECTED,
    CONNECTION_LOST
  }
}
