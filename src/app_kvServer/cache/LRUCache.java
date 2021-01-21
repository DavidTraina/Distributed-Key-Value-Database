package app_kvServer.cache;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;

public class LRUCache extends Cache {

  private final Map<String, String> lruCache;

  public LRUCache(int maxSize) {
    this.lruCache =
        Collections.synchronizedMap(
            new LinkedHashMap<>(maxSize, 0.75f, true) {
              @Override
              protected boolean removeEldestEntry(Map.Entry<String, String> eldest) {
                return lruCache.size() > maxSize;
              }
            });
    super.maxSize = maxSize;
  }

  /**
   * Get the value associated with the key
   *
   * @param key
   * @return value associated with key
   * @throws NoSuchElementException when key not in the key range of the store
   */
  @Override
  public String getKV(String key) throws NoSuchElementException {
    return Optional.ofNullable(lruCache.get(key))
        .orElseThrow(() -> new NoSuchElementException("Key not found"));
  }

  /**
   * Put the key-value pair into the store
   *
   * @param key
   * @param value
   * @throws Exception when key not in the key range of the store
   */
  @Override
  public void putKV(String key, String value) throws Exception { // todo exception
    lruCache.put(key, value);
  }
}
