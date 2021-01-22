package app_kvServer.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.util.NoSuchElementException;
import java.util.Optional;

public class ConcurrentCache<K, V> extends ThreadSafeCache<K, V> {
  private final Cache<K, V> concurrentCache;

  /**
   * Create a concurrent ThreadSafeCache with a Window TinyLfu eviction policy. See
   * https://github.com/ben-manes/caffeine/wiki/Efficiency#window-tinylfu for details. Pretty much
   * the best cache around.
   *
   * @param maxSize The maximum number of elements the cache can hold.
   */
  public ConcurrentCache(int maxSize) {
    super(maxSize);
    concurrentCache =
        Caffeine.newBuilder().maximumSize(maxSize).initialCapacity(maxSize / 2).build();
  }

  @Override
  public V get(K key) throws NoSuchElementException {
    return Optional.ofNullable(concurrentCache.getIfPresent(key))
        .orElseThrow(() -> new NoSuchElementException("Key not found"));
  }

  @Override
  public void put(K key, V value) {
    concurrentCache.put(key, value);
  }
}
