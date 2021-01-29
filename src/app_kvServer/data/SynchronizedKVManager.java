package app_kvServer.data;

import app_kvServer.data.cache.CacheStrategy;
import app_kvServer.data.cache.ThreadSafeCache;
import app_kvServer.data.cache.ThreadSafeCacheFactory;
import app_kvServer.data.storage.DiskStorage;
import app_kvServer.data.storage.DiskStorageException;
import java.util.NoSuchElementException;
import shared.messages.KVMessage;

public final class SynchronizedKVManager {
  private static SynchronizedKVManager INSTANCE;
  private final ThreadSafeCache<String, String> cache;
  private final DiskStorage diskStorage;

  private SynchronizedKVManager(final int cacheSize, final CacheStrategy cacheStrategy) {
    cache = new ThreadSafeCacheFactory<String, String>().getCache(cacheSize, cacheStrategy);
    try {
      diskStorage = new DiskStorage();
    } catch (DiskStorageException e) {
      // TODO What do we do when there is a problem with storage?
      throw new ExceptionInInitializerError(e);
    }
  }

  public static SynchronizedKVManager getInstance() {
    if (INSTANCE == null) {
      throw new AssertionError("SynchronizedKVManager is uninitialized.");
    }
    return INSTANCE;
  }

  public static synchronized void initialize(
      final int cacheSize, final CacheStrategy cacheStrategy) {
    if (INSTANCE != null) {
      throw new AssertionError("Instance has already been initialized.");
    }
    INSTANCE = new SynchronizedKVManager(cacheSize, cacheStrategy);
  }

  public synchronized KVMessage handleRequest(final KVMessage request) {
    switch (request.getStatus()) {
      case GET:
        return getKV(request);
      case PUT:
        return putKV(request);
      default:
        return new KVMessage("Request type is invalid", null, KVMessage.StatusType.FAILED);
    }
  }

  public synchronized void clearCache() {
    cache.purge();
  }

  public CacheStrategy getCacheStrategy() {
    return cache.getStrategy();
  }

  private synchronized KVMessage getKV(final KVMessage request) throws NoSuchElementException {
    try {
      return new KVMessage(
          request.getKey(), cache.get(request.getKey()), KVMessage.StatusType.GET_SUCCESS);
    } catch (NoSuchElementException e) {
      final KVMessage kvMessage = diskStorage.get(request);
      if (kvMessage.getStatus() == KVMessage.StatusType.GET_SUCCESS) {
        cache.put(request.getKey(), kvMessage.getValue());
      }
      return kvMessage;
    }
  }

  private synchronized KVMessage putKV(final KVMessage request) {
    if (request.getValue() == null) {
      cache.remove(request.getKey());
    } else {
      cache.put(request.getKey(), request.getValue());
    }
    return diskStorage.put(request);
  }
}
