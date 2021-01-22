package app_kvServer.cache;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;

public class SynchronizedLRUCache<K, V> extends ThreadSafeCache<K, V> {

  private final Map<K, V> lruCache;

  /**
   * Create a synchronized ThreadSafeCache with a least-recently-used eviction policy and the
   * specified maximum size.
   *
   * @param maxSize The maximum number of elements the cache can hold.
   */
  public SynchronizedLRUCache(int maxSize) {
    super(maxSize);
    this.lruCache =
        Collections.synchronizedMap(
            new LinkedHashMap<>(maxSize / 2, 0.75f, true) {
              @Override
              protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
                return lruCache.size() > maxSize;
              }
            });
  }

  @Override
  public V get(K key) throws NoSuchElementException {
    return Optional.ofNullable(lruCache.get(key))
        .orElseThrow(() -> new NoSuchElementException("Key not found"));
  }

  @Override
  public void put(K key, V value) { // todo exception
    lruCache.put(key, value);
  }
}
