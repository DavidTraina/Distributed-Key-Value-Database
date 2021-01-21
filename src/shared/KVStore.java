package shared;

import java.util.NoSuchElementException;

public interface KVStore<K, V> {
  /**
   * Get the value associated with the key
   *
   * @return value associated with key
   * @throws NoSuchElementException when key not in the key range of the store
   */
  V getKV(K key) throws NoSuchElementException;

  /**
   * Put the key-value pair into the store
   *
   * @throws Exception when key not in the key range of the store
   */
  void putKV(K key, V value) throws Exception;
}
