package app_kvServer.data.cache;

public class ThreadSafeCacheFactory<K, V> {
  /**
   * Return the specified ThreadSafeCache implementation.
   *
   * @param maxSize The maximum number of elements the cache can hold.
   * @param strategy Specifies an implementation of ThreadSafeCache.
   * @return A ThreadSafeCache implementation corresponding to strategy.
   */
  public ThreadSafeCache<K, V> getCache(final int maxSize, final CacheStrategy strategy) {
    switch (strategy) {
      case LRU:
        return new SynchronizedLRUCache<>(maxSize);
      case LFU:
        return new SynchronizedLFUCache<>(maxSize);
      case FIFO:
        return new SynchronizedFIFOCache<>(maxSize);
      default: // CONCURRENT
        return new ConcurrentCache<>(maxSize);
    }
  }
}
