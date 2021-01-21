package client;

import shared.messages.KVMessage;

public class KVStoreClient implements KVCommInterface {
  /**
   * Initialize KVStore with address and port of KVServer
   *
   * @param address the address of the KVServer
   * @param port the port of the KVServer
   */
  public KVStoreClient(String address, int port) {
    // TODO Auto-generated method stub
  }

  @Override
  public void connect() throws Exception {
    // TODO Auto-generated method stub
  }

  @Override
  public void disconnect() {
    // TODO Auto-generated method stub
  }

  @Override
  public KVMessage put(String key, String value) throws Exception {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public KVMessage get(String key) throws Exception {
    // TODO Auto-generated method stub
    return null;
  }
}
