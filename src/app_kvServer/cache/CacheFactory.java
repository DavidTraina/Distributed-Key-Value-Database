package app_kvServer.cache;

import app_kvServer.IKVServer;

public class CacheFactory {
  public static Cache getCache(int maxSize, IKVServer.CacheStrategy strategy)
      throws IllegalArgumentException {
    switch (strategy) {
      case LRU:
        return new LRUCache(maxSize);
      case LFU:
        return new LFUCache(maxSize);
      case FIFO:
        return new FIFOCache(maxSize);
      default:
        throw new IllegalArgumentException("Not a valid strategy.");
    }
  }
}
