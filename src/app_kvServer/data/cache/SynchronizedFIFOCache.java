package app_kvServer.data.cache;

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
  public SynchronizedFIFOCache(final int maxSize) {
    super(maxSize, CacheStrategy.FIFO);
    this.fifoCache =
        Collections.synchronizedMap(
            new LinkedHashMap<K, V>(maxSize / 2, 0.75f, false) {
              @Override
              protected boolean removeEldestEntry(final Map.Entry<K, V> eldest) {
                return fifoCache.size() > maxSize;
              }
            });
  }

  @Override
  public V get(final K key) throws NoSuchElementException {
    return Optional.ofNullable(fifoCache.get(key))
        .orElseThrow(() -> new NoSuchElementException("Key not found"));
  }

  @Override
  public void put(final K key, final V value) {
    fifoCache.put(key, value);
  }

  @Override
  public void remove(K key) {
    fifoCache.remove(key);
  }

  @Override
  public boolean containsKey(final K key) {
    return fifoCache.containsKey(key);
  }

  @Override
  public void purge() {
    fifoCache.clear();
  }
}
