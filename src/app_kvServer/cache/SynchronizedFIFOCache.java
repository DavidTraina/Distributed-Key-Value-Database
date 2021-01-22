package app_kvServer.cache;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;

public class SynchronizedFIFOCache<K, V> extends ThreadSafeCache<K, V> {

  private final Map<K, V> fifoCache;

  /**
   * Return a synchronized ThreadSafeCache with a first-in-first-out eviction policy and the
   * specified maximum size.
   *
   * @param maxSize The maximum number of elements the cache can hold.
   */
  public SynchronizedFIFOCache(int maxSize) {
    super(maxSize);
    this.fifoCache =
        Collections.synchronizedMap(
            new LinkedHashMap<K, V>(maxSize / 2, 0.75f, false) {
              @Override
              protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
                return fifoCache.size() > maxSize;
              }
            });
  }

  @Override
  public V get(K key) throws NoSuchElementException {
    return Optional.ofNullable(fifoCache.get(key))
        .orElseThrow(() -> new NoSuchElementException("Key not found"));
  }

  @Override
  public void put(K key, V value) {
    fifoCache.put(key, value);
  }
}
