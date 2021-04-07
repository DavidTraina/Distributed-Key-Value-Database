package app_kvServer.data;

import static shared.communication.messages.DataTransferMessage.DataTransferMessageType.*;

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
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
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
      final int cacheSize,
      final CacheStrategy cacheStrategy,
      final String nodeName,
      boolean encrypted) {
    cache = new ThreadSafeCacheFactory<String, String>().getCache(cacheSize, cacheStrategy);
    try {
      diskStorage = new DiskStorage(nodeName, encrypted);
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
      final int cacheSize,
      final CacheStrategy cacheStrategy,
      final String nodeName,
      boolean encrypted) {
    if (INSTANCE != null) {
      throw new AssertionError("Instance has already been initialized.");
    }
    INSTANCE = new SynchronizedKVManager(cacheSize, cacheStrategy, nodeName, encrypted);
  }

  public synchronized void setWriteEnabled(boolean writeEnabled) {
    this.writeEnabled.set(writeEnabled);
  }

  public synchronized KVMessage handleClientRequest(final KVMessage request) {
    if (!messageIsValidSize(request)) {
      return new KVMessage(
          request.getKey(),
          request.getValue(),
          KVMessage.StatusType.FAILED,
          request.getRequestId());
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
            request.getRequestId());
    }
  }

  public synchronized KVMessage handleServerRequest(final KVMessage request) {
    if (request.getStatus() == KVMessage.StatusType.PUT) {
      logger.info("Received a replication PUT request for key: " + request.getKey());
      return handleReplication(request);
    }
    return new KVMessage(
        request.getKey(), request.getValue(), KVMessage.StatusType.FAILED, request.getRequestId());
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
          request.getKey(),
          request.getValue(),
          KVMessage.StatusType.NOT_RESPONSIBLE,
          request.getRequestId());
    }
    return diskStorage.put(request, storageType);
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
        hashRange, DiskStorage.StorageType.SELF, true);
  }

  public DataTransferMessage getDataChunkForReplication(String[] hashRange) {
    return this.diskStorage.partitionDatabaseAndGetKeysInRange(
        hashRange, DiskStorage.StorageType.SELF, false);
  }

  public DataTransferMessage handleDataTransfer(DataTransferMessage dataTransferMessage) {
    switch (dataTransferMessage.getDataTransferMessageType()) {
      case DATA_TRANSFER_REQUEST:
        return this.diskStorage.updateDatabaseWithKVDataTransfer(
            dataTransferMessage, dataTransferMessage.getStorageType());

      case MOVE_REPLICA2_TO_REPLICA1:
        DataTransferMessage replica2Data =
            this.diskStorage.partitionDatabaseAndGetKeysInRange(
                dataTransferMessage.getHashRange(), DiskStorage.StorageType.REPLICA_2, true);
        replica2Data.setStorageType(DiskStorage.StorageType.REPLICA_1);
        return this.diskStorage.updateDatabaseWithKVDataTransfer(
            replica2Data, replica2Data.getStorageType());

      case MOVE_REPLICA1_TO_REPLICA2:
        DataTransferMessage replica1Data =
            this.diskStorage.partitionDatabaseAndGetKeysInRange(
                dataTransferMessage.getHashRange(), DiskStorage.StorageType.REPLICA_1, true);
        replica1Data.setStorageType(DiskStorage.StorageType.REPLICA_2);
        return this.diskStorage.updateDatabaseWithKVDataTransfer(
            replica1Data, replica1Data.getStorageType());

      case DELETE_DATA:
        String[] hashRangeToDelete = dataTransferMessage.getHashRange();
        DiskStorage.StorageType replicaToDeleteFrom = dataTransferMessage.getStorageType();
        DataTransferMessage deletionResponse =
            this.diskStorage.partitionDatabaseAndGetKeysInRange(
                hashRangeToDelete, replicaToDeleteFrom, true);
        if (deletionResponse.getDataTransferMessageType() == DATA_TRANSFER_REQUEST) {
          return new DataTransferMessage(
              DATA_TRANSFER_SUCCESS, "Deletion request on node " + this.nodeName + " successful.");
        } else {
          return new DataTransferMessage(
              DATA_TRANSFER_FAILURE,
              "Deletion request on node "
                  + this.nodeName
                  + " failed, with message: "
                  + deletionResponse.getMessage());
        }

      default:
        return new DataTransferMessage(
            DATA_TRANSFER_FAILURE,
            "Invalid message type " + dataTransferMessage.getDataTransferMessageType());
    }
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
          request.getRequestId());
    }
    try {
      return new KVMessage(
          request.getKey(),
          cache.get(request.getKey()),
          KVMessage.StatusType.GET_SUCCESS,
          request.getRequestId());
    } catch (NoSuchElementException e) {
      DiskStorage.StorageType storage = returnReplicaType(request);
      if (storage == null) {
        storage = DiskStorage.StorageType.SELF;
      }
      final KVMessage result = diskStorage.get(request, storage);
      if (result.getStatus() == KVMessage.StatusType.GET_SUCCESS) {
        cache.put(request.getKey(), result.getValue());
      }
      return result;
    }
  }

  private DiskStorage.StorageType returnReplicaType(KVMessage request) {
    String key = request.getKey();
    ECSNode[] replicas =
        ECSMetadata.getInstance().getNodesWhereIAmReplicaBasedOnName(this.nodeName);
    logger.debug(
        String.format(
            "Nodes where %s is a replica: %s",
            this.nodeName,
            Arrays.stream(replicas).map(ECSNode::getNodeName).collect(Collectors.toList())));

    if (replicas == null || replicas.length == 0) {
      logger.error("Could not find any replicas");
      return null;
    }
    if (replicas.length == 2) {
      if (ECSUtils.checkIfKeyBelongsInRange(key, replicas[0].getNodeHashRange())) {
        return DiskStorage.StorageType.REPLICA_1;
      } else if (ECSUtils.checkIfKeyBelongsInRange(key, replicas[1].getNodeHashRange())) {
        return DiskStorage.StorageType.REPLICA_2;
      } else {
        logger.error("Key doesn't belong to any known replica!!");
        return null;
      }
    } else if (replicas.length == 1) {
      if (ECSUtils.checkIfKeyBelongsInRange(key, replicas[0].getNodeHashRange())) {
        return DiskStorage.StorageType.REPLICA_1;
      } else {
        logger.error("Key doesn't belong to any known replica!!");
        return null;
      }
    } else {
      logger.error("Too many replicas!");
      return null;
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
    // If GET, check if node has replica which can service request
    if (request.getStatus() == KVMessage.StatusType.GET) {
      ECSNode[] replicas =
          ECSMetadata.getInstance().getNodesWhereIAmReplicaBasedOnName(this.nodeName);
      return ECSUtils.checkIfKeyBelongsInRange(key, identityNode.getNodeHashRange())
          || Arrays.stream(replicas)
              .anyMatch(
                  replica -> ECSUtils.checkIfKeyBelongsInRange(key, replica.getNodeHashRange()));
    }
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
          request.getRequestId());
    }
    if (!writingIsAvailable()) {
      logger.info("Writing is not available to serve request: " + request.toString());
      return new KVMessage(
          request.getKey(),
          request.getValue(),
          KVMessage.StatusType.SERVER_WRITE_LOCK,
          request.getRequestId());
    }
    if (request.getValue() == null) {
      logger.info("Deleting key from cache");
      cache.remove(request.getKey());
    } else {
      logger.info("Adding key to cache");
      cache.put(request.getKey(), request.getValue());
    }
    logger.info("Accessing disk storage for key");
    return diskStorage.put(request, DiskStorage.StorageType.SELF);
  }

  public synchronized boolean moveReplicaDataToSelfStorage(String[] hashRange) {
    HashMap<String, String> replica1Data =
        diskStorage
            .partitionDatabaseAndGetKeysInRange(hashRange, DiskStorage.StorageType.REPLICA_1, true)
            .getPayload();
    HashMap<String, String> replica2Data =
        diskStorage
            .partitionDatabaseAndGetKeysInRange(hashRange, DiskStorage.StorageType.REPLICA_2, true)
            .getPayload();
    boolean success = true;
    success = moveReplicaDataToSelf(replica1Data, success);
    success = moveReplicaDataToSelf(replica2Data, success);
    return success;
  }

  private boolean moveReplicaDataToSelf(HashMap<String, String> replica1Data, boolean success) {
    for (Map.Entry<String, String> entry : replica1Data.entrySet()) {
      KVMessage response =
          diskStorage.put(
              new KVMessage(entry.getKey(), entry.getValue(), KVMessage.StatusType.PUT),
              DiskStorage.StorageType.SELF);
      success =
          success
              && (response.getStatus() == KVMessage.StatusType.PUT_SUCCESS
                  || response.getStatus() == KVMessage.StatusType.PUT_UPDATE);
    }
    return success;
  }

  private synchronized boolean messageIsValidSize(final KVMessage request) {
    return request.getKey().getBytes(StandardCharsets.UTF_8).length <= MAX_KEY_BYTES
        && (request.getValue() == null
            || request.getValue().getBytes(StandardCharsets.UTF_8).length <= MAX_VALUE_BYTES);
  }

  private synchronized boolean writingIsAvailable() {
    return this.writeEnabled.get();
  }
}
