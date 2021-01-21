package app_kvServer.cache;

public class LFUCache extends Cache {

  public LFUCache(int maxSize) {
    super();
  }

  /**
   * Get the value associated with the key
   *
   * @param key
   * @return value associated with key
   * @throws Exception when key not in the key range of the store
   */
  @Override
  public String getKV(String key) throws Exception {
    return null;
  }

  /**
   * Put the key-value pair into the store
   *
   * @param key
   * @param value
   * @throws Exception when key not in the key range of the store
   */
  @Override
  public void putKV(String key, String value) throws Exception {}
}
