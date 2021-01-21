package app_kvServer.cache;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;

public class FIFOCache extends Cache {

  private final Map<String, String> fifoCache;

  public FIFOCache(int maxSize) {
    this.fifoCache =
        Collections.synchronizedMap(
            new LinkedHashMap<>(maxSize, 0.75f, false) {
              @Override
              protected boolean removeEldestEntry(Map.Entry<String, String> eldest) {
                return fifoCache.size() > maxSize;
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
  public String getKV(String key) throws NoSuchElementException { // todo can change signature?
    return Optional.ofNullable(fifoCache.get(key))
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
    fifoCache.put(key, value);
  }
}
