package app_kvServer.data.cache;

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
  public SynchronizedLRUCache(final int maxSize) {
    super(maxSize, CacheStrategy.LRU);
    lruCache =
        Collections.synchronizedMap(
            new LinkedHashMap<K, V>(maxSize / 2, 0.75f, true) {
              @Override
              protected boolean removeEldestEntry(final Map.Entry<K, V> eldest) {
                return lruCache.size() > maxSize;
              }
            });
  }

  @Override
  public V get(final K key) throws NoSuchElementException {
    return Optional.ofNullable(lruCache.get(key))
        .orElseThrow(() -> new NoSuchElementException("Key: \"" + key.toString() + "\" not found"));
  }

  @Override
  public void put(final K key, final V value) { // todo exception
    lruCache.put(key, value);
  }

  @Override
  public boolean containsKey(final K key) {
    return lruCache.containsKey(key);
  }

  @Override
  public void purge() {
    lruCache.clear();
  }
}
