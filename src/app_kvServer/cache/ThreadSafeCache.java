package app_kvServer.cache;

import java.util.NoSuchElementException;

/**
 * An thread-safe in-memory key-value store with a maximum size. Eviction policy up to
 * implementation.
 *
 * @param <K> The type of the keys in the cache.
 * @param <V> The type of values in the cache.
 */
public abstract class ThreadSafeCache<K, V> {
  private final int maxSize;

  /**
   * Set the maximum size of the cache.
   *
   * @param maxSize The maximum number of elements the cache can hold.
   */
  protected ThreadSafeCache(int maxSize) {
    this.maxSize = maxSize;
  }

  /** @return The maximum number of elements the cache can hold. */
  public final int getMaxSize() {
    return maxSize;
  }

  /**
   * Retrieve the value in the cache for key.
   *
   * @param key The key to look up.
   * @return The value for key.
   * @throws NoSuchElementException when key is not present in teh cache.
   */
  public abstract V get(K key) throws NoSuchElementException;

  /**
   * Create a mapping from key to value in the cache.
   *
   * @param key The key for the mapping
   * @param value The value for the mapping.
   */
  public abstract void put(K key, V value);
}
