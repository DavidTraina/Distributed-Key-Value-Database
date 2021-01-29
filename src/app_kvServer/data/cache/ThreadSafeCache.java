package app_kvServer.data.cache;

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
  private final CacheStrategy strategy;

  /**
   * Set the maximum size of the cache.
   *
   * @param maxSize The maximum number of elements the cache can hold.
   */
  protected ThreadSafeCache(final int maxSize, final CacheStrategy strategy) {
    this.maxSize = maxSize;
    this.strategy = strategy;
  }

  /** @return The maximum number of elements the cache can hold. */
  public final int getMaxSize() {
    return maxSize;
  }

  /** @return The caching strategy associated with the implementation. */
  public final CacheStrategy getStrategy() {
    return strategy;
  }

  /**
   * Retrieve the value in the cache for key.
   *
   * @param key The key to look up.
   * @return The value for key.
   * @throws NoSuchElementException when key is not present in teh cache.
   */
  public abstract V get(final K key) throws NoSuchElementException;

  /**
   * Create a mapping from key to value in the cache.
   *
   * @param key The key for the mapping
   * @param value The value for the mapping.
   */
  public abstract void put(final K key, final V value);

  /**
   * Remove the mapping for key if it exists.
   *
   * @param key The key for the mapping
   */
  public abstract void remove(final K key);

  /**
   * Return true iff key is a key in the cache.
   *
   * @param key The key to look for.
   * @return true iff key is a key in the cache.
   */
  public abstract boolean containsKey(final K key);

  /** Clear the local cache of the server */
  public abstract void purge();
}
