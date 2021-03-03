package app_kvServer.data;

import app_kvServer.data.cache.CacheStrategy;
import app_kvServer.data.cache.ThreadSafeCache;
import app_kvServer.data.cache.ThreadSafeCacheFactory;
import app_kvServer.data.storage.DiskStorage;
import app_kvServer.data.storage.DiskStorageException;
import ecs.ECSMetadata;
import ecs.ECSNode;
import ecs.ECSUtils;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.log4j.Logger;
import shared.communication.messages.DataTransferMessage;
import shared.communication.messages.KVMessage;

public final class SynchronizedKVManager {
  private static final int MAX_KEY_BYTES = 20; // 20 Bytes
  private static final int MAX_VALUE_BYTES = 120 * 1024; // 120 KB
  private static final Logger logger = Logger.getLogger(SynchronizedKVManager.class);
  private static SynchronizedKVManager INSTANCE;
  private final ThreadSafeCache<String, String> cache;
  private final DiskStorage diskStorage;
  private final AtomicBoolean writeEnabled = new AtomicBoolean(true);
  private final String nodeName;

  private SynchronizedKVManager(
      final int cacheSize, final CacheStrategy cacheStrategy, final String nodeName) {
    cache = new ThreadSafeCacheFactory<String, String>().getCache(cacheSize, cacheStrategy);
    try {
      diskStorage = new DiskStorage(nodeName);
    } catch (DiskStorageException e) {
      // TODO What do we do when there is a problem with storage?
      throw new ExceptionInInitializerError(e);
    }
    this.nodeName = nodeName;
  }

  public static SynchronizedKVManager getInstance() {
    if (INSTANCE == null) {
      throw new AssertionError("SynchronizedKVManager is uninitialized.");
    }
    return INSTANCE;
  }

  public static synchronized void initialize(
      final int cacheSize, final CacheStrategy cacheStrategy, final String nodeName) {
    if (INSTANCE != null) {
      throw new AssertionError("Instance has already been initialized.");
    }
    INSTANCE = new SynchronizedKVManager(cacheSize, cacheStrategy, nodeName);
  }

  public synchronized void setWriteEnabled(boolean writeEnabled) {
    this.writeEnabled.set(writeEnabled);
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
        logger.info("Received a GET request for key: " + request.getKey());
        return getKV(request);
      case PUT:
        logger.info("Received a PUT request for key: " + request.getKey());
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
    if (!checkNodeResponsibleForRequest(request)) {
      logger.info(
          "Node not responsible for request with key: "
              + request.getKey()
              + " hash: "
              + ECSUtils.calculateMD5Hash(request.getKey()));
      return new KVMessage(
          request.getKey(),
          request.getValue(),
          KVMessage.StatusType.NOT_RESPONSIBLE,
          "Node Not Responsible",
          ECSMetadata.getInstance());
    }
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

  private boolean checkNodeResponsibleForRequest(KVMessage request) {
    String key = request.getKey();
    ECSNode identityNode = ECSMetadata.getInstance().getNodeBasedOnName(this.nodeName);
    if (identityNode == null) {
      logger.error("Could not find node by name!!");
      return false;
    }
    logger.info(
        String.format(
            "Received key %s with hash %s; Node hash range %s",
            key, ECSUtils.calculateMD5Hash(key), Arrays.toString(identityNode.getNodeHashRange())));
    return ECSUtils.checkIfKeyBelongsInRange(key, identityNode.getNodeHashRange());
  }

  private synchronized KVMessage putKV(final KVMessage request) {
    if (!checkNodeResponsibleForRequest(request)) {
      logger.info(
          "Node not responsible for request with key: "
              + request.getKey()
              + " hash: "
              + ECSUtils.calculateMD5Hash(request.getKey()));
      return new KVMessage(
          request.getKey(),
          request.getValue(),
          KVMessage.StatusType.NOT_RESPONSIBLE,
          "Node Not Responsible",
          ECSMetadata.getInstance());
    }
    if (!writingIsAvailable()) {
      logger.info("Writing is not available to serve request: " + request.toString());
      return new KVMessage(
          request.getKey(),
          request.getValue(),
          KVMessage.StatusType.SERVER_WRITE_LOCK,
          "No write requests can be processed at the moment");
    }
    if (request.getValue() == null) {
      logger.info("Deleting key from cache");
      cache.remove(request.getKey());
    } else {
      logger.info("Adding key to cache");
      cache.put(request.getKey(), request.getValue());
    }
    logger.info("Accessing disk storage for key");
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
