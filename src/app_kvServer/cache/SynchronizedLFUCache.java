package app_kvServer.cache;

import java.util.Collections;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import org.apache.activemq.util.LFUCache;

public class SynchronizedLFUCache<K, V> extends ThreadSafeCache<K, V> {

  private final Map<K, V> lfuCache;

  /**
   * Create a synchronized ThreadSafeCache with a least-frequently-used eviction policy and the
   * specified maximum size. Based on * http://dhruvbird.com/lfu.pdf to achieve amortized O(1) get()
   * and put() ops.
   *
   * @param maxSize The maximum number of elements the cache can hold.
   */
  public SynchronizedLFUCache(int maxSize) {
    super(maxSize);
    // When cache reaches max capacity, ceil(maxCacheSize * evictionFactor) of the lowest frequency
    // entries are deleted. Here we ensure we evict exactly 1 entry for a true LFU implementation.
    lfuCache = Collections.synchronizedMap(new LFUCache<>(maxSize, (float) 1 / (2 * maxSize)));
  }

  @Override
  public V get(K key) throws NoSuchElementException {
    return Optional.ofNullable(lfuCache.get(key))
        .orElseThrow(() -> new NoSuchElementException("Key not found"));
  }

  @Override
  public void put(K key, V value) {
    lfuCache.put(key, value);
  }
}
