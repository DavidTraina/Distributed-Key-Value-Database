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
import shared.communication.messages.ClientKVMessage;
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

  public synchronized ClientKVMessage handleClientRequest(final ClientKVMessage request) {
    if (!messageIsValidSize(request)) {
      return new ClientKVMessage(
          request.getKey(),
          request.getValue(),
          ClientKVMessage.StatusType.FAILED,
          "Message too large");
    }
    switch (request.getStatus()) {
      case GET:
        logger.info("Received a GET request for key: " + request.getKey());
        return getKV(request);
      case PUT:
        logger.info("Received a PUT request for key: " + request.getKey());
        return putKV(request);
      default:
        return new ClientKVMessage(
            request.getKey(),
            request.getValue(),
            ClientKVMessage.StatusType.FAILED,
            "Request type invalid");
    }
  }

  public synchronized KVMessage handleServerRequest(final KVMessage request) {
    switch (request.getStatus()) {
      case PUT:
        logger.info("Received a replication PUT request for key: " + request.getKey());
        return handleReplication(request);
      default:
        return new KVMessage(
            request.getKey(), request.getValue(), ClientKVMessage.StatusType.FAILED);
    }
  }

  private KVMessage handleReplication(KVMessage request) {
    DiskStorage.StorageType storageType = returnReplicaType(request);
    if (storageType == null) {
      logger.info(
          "Node not responsible for replication of the request with key: "
              + request.getKey()
              + " hash: "
              + ECSUtils.calculateMD5Hash(request.getKey()));
      return new KVMessage(
          request.getKey(), request.getValue(), ClientKVMessage.StatusType.NOT_RESPONSIBLE);
    }
    return diskStorage.put(request, DiskStorage.StorageType.SELF);
  }

  public synchronized void clearCache() {
    cache.purge();
  }

  public CacheStrategy getCacheStrategy() {
    return cache.getStrategy();
  }

  public DataTransferMessage partitionDatabaseAndGetKeysInRange(String[] hashRange) {
    clearCache();
    return this.diskStorage.partitionDatabaseAndGetKeysInRange(
        hashRange, DiskStorage.StorageType.SELF);
  }

  public DataTransferMessage handleDataTransfer(DataTransferMessage dataTransferMessage) {
    return this.diskStorage.updateDatabaseWithKVDataTransfer(
        dataTransferMessage, DiskStorage.StorageType.SELF);
  }

  private synchronized ClientKVMessage getKV(final ClientKVMessage request)
      throws NoSuchElementException {
    if (!checkNodeResponsibleForRequest(request)) {
      logger.info(
          "Node not responsible for request with key: "
              + request.getKey()
              + " hash: "
              + ECSUtils.calculateMD5Hash(request.getKey()));
      return new ClientKVMessage(
          request.getKey(),
          request.getValue(),
          ClientKVMessage.StatusType.NOT_RESPONSIBLE,
          "Node Not Responsible",
          ECSMetadata.getInstance());
    }
    try {
      return new ClientKVMessage(
          request.getKey(), cache.get(request.getKey()), ClientKVMessage.StatusType.GET_SUCCESS);
    } catch (NoSuchElementException e) {
      final KVMessage kvMessage = diskStorage.get(request, DiskStorage.StorageType.SELF);
      if (kvMessage.getStatus() == ClientKVMessage.StatusType.GET_SUCCESS) {
        cache.put(request.getKey(), kvMessage.getValue());
      }
      return new ClientKVMessage(kvMessage);
    }
  }

  private DiskStorage.StorageType returnReplicaType(KVMessage request) {
    String key = request.getKey();
    ECSNode[] replicas = ECSMetadata.getInstance().getReplicasBasedOnName(this.nodeName);
    if (replicas == null) {
      logger.error("Could not find node by name!!");
      return null;
    }
    int i;
    for (i = 0; i < replicas.length; i++) {
      if (ECSUtils.checkIfKeyBelongsInRange(key, replicas[i].getNodeHashRange())) {
        logger.info(
            String.format(
                "Received replication request for key %s with hash %s",
                key, ECSUtils.calculateMD5Hash(key)));
        break;
      }
    }
    switch (i) {
      case 0:
        return DiskStorage.StorageType.REPLICA_1;
      case 1:
        return DiskStorage.StorageType.REPLICA_2;
      default:
        logger.error("Key doesn't belong to any known replica!!");
        return null;
    }
  }

  private boolean checkNodeResponsibleForRequest(ClientKVMessage request) {
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

  private synchronized ClientKVMessage putKV(final ClientKVMessage request) {
    if (!checkNodeResponsibleForRequest(request)) {
      logger.info(
          "Node not responsible for request with key: "
              + request.getKey()
              + " hash: "
              + ECSUtils.calculateMD5Hash(request.getKey()));
      return new ClientKVMessage(
          request.getKey(),
          request.getValue(),
          ClientKVMessage.StatusType.NOT_RESPONSIBLE,
          "Node Not Responsible",
          ECSMetadata.getInstance());
    }
    if (!writingIsAvailable()) {
      logger.info("Writing is not available to serve request: " + request.toString());
      return new ClientKVMessage(
          request.getKey(),
          request.getValue(),
          ClientKVMessage.StatusType.SERVER_WRITE_LOCK,
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
    return new ClientKVMessage(diskStorage.put(request, DiskStorage.StorageType.SELF));
  }

  private synchronized boolean messageIsValidSize(final ClientKVMessage request) {
    return request.getKey().getBytes(StandardCharsets.UTF_8).length <= MAX_KEY_BYTES
        && (request.getValue() == null
            || request.getValue().getBytes(StandardCharsets.UTF_8).length <= MAX_VALUE_BYTES);
  }

  private synchronized boolean writingIsAvailable() {
    return this.writeEnabled.get();
  }
}
