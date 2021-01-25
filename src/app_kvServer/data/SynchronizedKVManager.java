package app_kvServer.data;

import app_kvServer.data.cache.CacheStrategy;
import app_kvServer.data.cache.ThreadSafeCache;
import app_kvServer.data.cache.ThreadSafeCacheFactory;
import java.util.NoSuchElementException;
import shared.messages.KVMessage;

public final class SynchronizedKVManager {
  private static SynchronizedKVManager INSTANCE;
  private final ThreadSafeCache<String, String> cache;

  private SynchronizedKVManager(final int cacheSize, final CacheStrategy cacheStrategy) {
    cache = new ThreadSafeCacheFactory<String, String>().getCache(cacheSize, cacheStrategy);
  }

  public static SynchronizedKVManager getInstance() {
    if (INSTANCE == null) {
      throw new AssertionError("SynchronizedKVManager is uninitialized.");
    }
    return INSTANCE;
  }

  public static synchronized void initialize(final int cacheSize, final CacheStrategy cacheStrategy)
      throws IllegalAccessException {
    if (INSTANCE != null) {
      throw new IllegalAccessException("Instance has already been initialized.");
    }
    INSTANCE = new SynchronizedKVManager(cacheSize, cacheStrategy);
  }

  public synchronized KVMessage handleRequest(final KVMessage request) {
    switch (request.getStatus()) {
      case GET:
        try {
          return getKV(request.getKey());
        } catch (NoSuchElementException e) {
          // TODO handle error, return according status type
        }
      case PUT:
        // TODO decide if it's PUT or UPDATE of DELETE and return according status type after op
      default:
        // TODO handle error
        break;
    }
    return null;
  }

  private synchronized KVMessage getKV(final String key) throws NoSuchElementException {
    try {
      return new KVMessage(key, cache.get(key), KVMessage.StatusType.GET_SUCCESS);
    } catch (NoSuchElementException e) {
      final KVMessage kvMessage = fetchFromDB(key);
      // TODO verify success
      cache.put(key, kvMessage.getValue());
      return kvMessage;
    }
  }

  private synchronized KVMessage putKV(final String key, final String value) {
    // TODO handle case where null value indicates deletion
    cache.put(key, value);
    return saveToDB(key, value);
  }

  private synchronized KVMessage fetchFromDB(final String key) {
    // TODO fetch from disk
    return null;
  }

  private synchronized KVMessage saveToDB(final String key, final String value) {
    // TODO save to disk
    return null;
  }
}
