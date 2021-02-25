package app_kvServer.data;

import app_kvServer.data.cache.CacheStrategy;
import app_kvServer.data.cache.ThreadSafeCache;
import app_kvServer.data.cache.ThreadSafeCacheFactory;
import app_kvServer.data.storage.DiskStorage;
import app_kvServer.data.storage.DiskStorageException;
import java.nio.charset.StandardCharsets;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;
import shared.communication.messages.DataTransferMessage;
import shared.communication.messages.KVMessage;

public final class SynchronizedKVManager {
  private static SynchronizedKVManager INSTANCE;
  private final ThreadSafeCache<String, String> cache;
  private final DiskStorage diskStorage;
  private static final int MAX_KEY_BYTES = 20; // 20 Bytes
  private static final int MAX_VALUE_BYTES = 120 * 1024; // 120 KB
  private final AtomicBoolean writeEnabled = new AtomicBoolean(true);

  public synchronized void setWriteEnabled(boolean writeEnabled) {
    this.writeEnabled.set(writeEnabled);
  }

  private SynchronizedKVManager(
      final int cacheSize, final CacheStrategy cacheStrategy, final int uniqueID) {
    cache = new ThreadSafeCacheFactory<String, String>().getCache(cacheSize, cacheStrategy);
    try {
      diskStorage = new DiskStorage(uniqueID);
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
      final int cacheSize, final CacheStrategy cacheStrategy, final int uniqueID) {
    if (INSTANCE != null) {
      throw new AssertionError("Instance has already been initialized.");
    }
    INSTANCE = new SynchronizedKVManager(cacheSize, cacheStrategy, uniqueID);
  }

  public synchronized KVMessage handleRequest(final KVMessage request) {
    if (!storageServiceIsRunning()) {
      return new KVMessage(
          request.getKey(),
          request.getValue(),
          KVMessage.StatusType.SERVER_STOPPED,
          "No requests can be processed at the moment");
    }
    if (!messageIsValidSize(request)) {
      return new KVMessage(
          request.getKey(), request.getValue(), KVMessage.StatusType.FAILED, "Message too large");
    }
    switch (request.getStatus()) {
      case GET:
        return getKV(request);
      case PUT:
        return putKV(request);
      default:
        return new KVMessage(
            request.getKey(),
            request.getValue(),
            KVMessage.StatusType.FAILED,
            "Request type invalid");
    }
  }

  public synchronized void clearCache() {
    cache.purge();
  }

  public CacheStrategy getCacheStrategy() {
    return cache.getStrategy();
  }

  public DataTransferMessage partitionDatabaseAndGetKeysInRange(String[] hashRange) {
    clearCache();
    return this.diskStorage.partitionDatabaseAndGetKeysInRange(hashRange);
  }

  public DataTransferMessage handleDataTransfer(DataTransferMessage dataTransferMessage) {
    return this.diskStorage.updateDatabaseWithKVDataTransfer(dataTransferMessage);
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
    if (!writingIsAvailable()) {
      return new KVMessage(
          request.getKey(),
          request.getValue(),
          KVMessage.StatusType.SERVER_WRITE_LOCK,
          "No write requests can be processed at the moment");
    }
    if (request.getValue() == null) {
      cache.remove(request.getKey());
    } else {
      cache.put(request.getKey(), request.getValue());
    }
    return diskStorage.put(request);
  }

  private synchronized boolean messageIsValidSize(final KVMessage request) {
    return request.getKey().getBytes(StandardCharsets.UTF_8).length <= MAX_KEY_BYTES
        && (request.getValue() == null
            || request.getValue().getBytes(StandardCharsets.UTF_8).length <= MAX_VALUE_BYTES);
  }

  private synchronized boolean storageServiceIsRunning() {
    // TODO: Check that storage service is running
    return true;
  }

  private synchronized boolean writingIsAvailable() {
    return this.writeEnabled.get();
  }

  private synchronized boolean checkWithinHashkeyRange() {
    // TODO: Check that the hash is within the server range
    return true;
  }
}
