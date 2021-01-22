package app_kvServer.cache;

import app_kvServer.IKVServer;

public class ThreadSafeCacheFactory<K, V> {
  /**
   * Return the specified ThreadSafeCache implementation.
   *
   * @param maxSize The maximum number of elements the cache can hold.
   * @param strategy Specifies an implementation of ThreadSafeCache.
   * @return A ThreadSafeCache implementation corresponding to strategy.
   * @throws IllegalArgumentException if an invalid strategy is supplied.
   */
  public ThreadSafeCache<K, V> getCache(int maxSize, IKVServer.CacheStrategy strategy)
      throws IllegalArgumentException {
    switch (strategy) {
      case LRU:
        return new SynchronizedLRUCache<>(maxSize);
      case LFU:
        return new SynchronizedLFUCache<>(maxSize);
      case FIFO:
        return new SynchronizedFIFOCache<>(maxSize);
      case CONCURRENT:
        return new ConcurrentCache<>(maxSize);
      default:
        throw new IllegalArgumentException("Not a valid strategy.");
    }
  }
}
