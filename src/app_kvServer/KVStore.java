package app_kvServer;

public interface KVStore {
  /**
   * Get the value associated with the key
   *
   * @return value associated with key
   * @throws Exception when key not in the key range of the store
   */
  String getKV(String key) throws Exception;

  /**
   * Put the key-value pair into the store
   *
   * @throws Exception when key not in the key range of the store
   */
  void putKV(String key, String value) throws Exception;
}
