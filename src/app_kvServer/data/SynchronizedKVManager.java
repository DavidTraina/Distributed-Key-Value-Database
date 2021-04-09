package app_kvServer.data;

import static shared.communication.messages.DataTransferMessage.DataTransferMessageType.DATA_TRANSFER_FAILURE;
import static shared.communication.messages.DataTransferMessage.DataTransferMessageType.DATA_TRANSFER_REQUEST;
import static shared.communication.messages.DataTransferMessage.DataTransferMessageType.DATA_TRANSFER_SUCCESS;
import static shared.communication.messages.KVMessage.StatusType.DELETE_SUCCESS;
import static shared.communication.messages.KVMessage.StatusType.NOTIFY;
import static shared.communication.messages.KVMessage.StatusType.PUT;
import static shared.communication.messages.KVMessage.StatusType.PUT_SUCCESS;
import static shared.communication.messages.KVMessage.StatusType.PUT_UPDATE;
import static shared.communication.messages.KVMessage.StatusType.SUBSCRIBE;
import static shared.communication.messages.KVMessage.StatusType.UNSUBSCRIBE;

import app_kvServer.KVServerConnection;
import app_kvServer.data.cache.CacheStrategy;
import app_kvServer.data.cache.ThreadSafeCache;
import app_kvServer.data.cache.ThreadSafeCacheFactory;
import app_kvServer.data.storage.DiskStorage;
import app_kvServer.data.storage.DiskStorageException;
import app_kvServer.data.storage.StorageUnit;
import ecs.ECSMetadata;
import ecs.ECSNode;
import ecs.ECSUtils;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.log4j.Logger;
import shared.communication.messages.DataTransferMessage;
import shared.communication.messages.ECSMessage;
import shared.communication.messages.KVMessage;
import shared.communication.security.Hashing;

public final class SynchronizedKVManager {
  private static final int MAX_KEY_BYTES = 20; // 20 Bytes
  private static final int MAX_VALUE_BYTES = 120 * 1024; // 120 KB
  private static final Logger logger = Logger.getLogger(SynchronizedKVManager.class);
  private static SynchronizedKVManager INSTANCE;
  private final ThreadSafeCache<String, String> cache;
  private final DiskStorage diskStorage;
  private final AtomicBoolean writeEnabled = new AtomicBoolean(true);
  private final String nodeName;
  private final ConcurrentHashMap<UUID, KVServerConnection> connections = new ConcurrentHashMap<>();

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

  public void addConnection(KVServerConnection connection) {
    logger.info("Adding connection: " + connection.getClientId());
    assert (connection.getClientId() != null);
    connections.put(connection.getClientId(), connection);
    logger.info("Connection added: " + connections.get(connection.getClientId()));
  }

  public synchronized void setWriteEnabled(boolean writeEnabled) {
    this.writeEnabled.set(writeEnabled);
  }

  public synchronized KVMessage handleClientRequest(final KVMessage request) {
    if (!messageIsValidSize(request)) {
      return new KVMessage(
          request.getKey(),
          request.getValue(),
          request.getClientId(),
          KVMessage.StatusType.FAILED,
          request.getRequestId());
    }
    switch (request.getStatus()) {
      case GET:
        logger.info("Received a GET request for key: " + request.getKey());
        return getKV(request);
      case PUT:
      case SUBSCRIBE:
      case UNSUBSCRIBE:
        logger.info("Received a " + request.getStatus() + " request for key: " + request.getKey());
        return writeKV(request);
      default:
        return new KVMessage(
            request.getKey(),
            request.getValue(),
            request.getClientId(),
            KVMessage.StatusType.FAILED,
            request.getRequestId());
    }
  }

  public synchronized KVMessage handleServerRequest(final KVMessage request) {
    if (request.getStatus() == PUT
        || request.getStatus() == SUBSCRIBE
        || request.getStatus() == UNSUBSCRIBE) {
      logger.info("Received a replication request for key: " + request.getKey());
      return handleReplication(request);
    }
    return new KVMessage(
        request.getKey(),
        request.getValue(),
        request.getClientId(),
        KVMessage.StatusType.FAILED,
        request.getRequestId());
  }

  private KVMessage handleReplication(KVMessage request) {
    DiskStorage.StorageType storageType = returnReplicaType(request);
    if (storageType == null) {
      logger.info(
          "Node not responsible for replication of the request with key: "
              + request.getKey()
              + " hash: "
              + Hashing.calculateMD5Hash(request.getKey()));
      return new KVMessage(
          request.getKey(),
          request.getValue(),
          request.getClientId(),
          KVMessage.StatusType.NOT_RESPONSIBLE,
          request.getRequestId());
    }
    return diskStorage.write(request, storageType).getKvMessageResponse();
  }

  public synchronized void clearCache() {
    cache.purge();
  }

  public CacheStrategy getCacheStrategy() {
    return cache.getStrategy();
  }

  public DataTransferMessage partitionDatabaseAndGetKeysInRange(
      ECSMessage ecsMessage, String[] hashRange) {
    clearCache();
    return this.diskStorage.partitionDatabaseAndGetKeysInRange(
        ecsMessage, hashRange, DiskStorage.StorageType.SELF, true);
  }

  public DataTransferMessage getDataChunkForReplication(ECSMessage ecsMessage, String[] hashRange) {
    return this.diskStorage.partitionDatabaseAndGetKeysInRange(
        ecsMessage, hashRange, DiskStorage.StorageType.SELF, false);
  }

  public DataTransferMessage handleDataTransfer(DataTransferMessage dataTransferMessage) {
    switch (dataTransferMessage.getDataTransferMessageType()) {
      case DATA_TRANSFER_REQUEST:
        return this.diskStorage.updateDatabaseWithKVDataTransfer(
            dataTransferMessage, dataTransferMessage.getStorageType());

      case MOVE_REPLICA2_TO_REPLICA1:
        DataTransferMessage replica2Data =
            this.diskStorage.partitionDatabaseAndGetKeysInRange(
                dataTransferMessage.getECSMessage(),
                dataTransferMessage.getHashRange(),
                DiskStorage.StorageType.REPLICA_2,
                true);
        replica2Data.setStorageType(DiskStorage.StorageType.REPLICA_1);
        return this.diskStorage.updateDatabaseWithKVDataTransfer(
            replica2Data, replica2Data.getStorageType());

      case MOVE_REPLICA1_TO_REPLICA2:
        DataTransferMessage replica1Data =
            this.diskStorage.partitionDatabaseAndGetKeysInRange(
                dataTransferMessage.getECSMessage(),
                dataTransferMessage.getHashRange(),
                DiskStorage.StorageType.REPLICA_1,
                true);
        replica1Data.setStorageType(DiskStorage.StorageType.REPLICA_2);
        return this.diskStorage.updateDatabaseWithKVDataTransfer(
            replica1Data, replica1Data.getStorageType());

      case DELETE_DATA:
        String[] hashRangeToDelete = dataTransferMessage.getHashRange();
        DiskStorage.StorageType replicaToDeleteFrom = dataTransferMessage.getStorageType();
        DataTransferMessage deletionResponse =
            this.diskStorage.partitionDatabaseAndGetKeysInRange(
                dataTransferMessage.getECSMessage(), hashRangeToDelete, replicaToDeleteFrom, true);
        if (deletionResponse.getDataTransferMessageType() == DATA_TRANSFER_REQUEST) {
          return new DataTransferMessage(
              DATA_TRANSFER_SUCCESS,
              "Deletion request on node " + this.nodeName + " successful.",
              dataTransferMessage.getECSMessage());
        } else {
          return new DataTransferMessage(
              DATA_TRANSFER_FAILURE,
              "Deletion request on node "
                  + this.nodeName
                  + " failed, with message: "
                  + deletionResponse.getMessage(),
              dataTransferMessage.getECSMessage());
        }

      default:
        return new DataTransferMessage(
            DATA_TRANSFER_FAILURE,
            "Invalid message type " + dataTransferMessage.getDataTransferMessageType(),
            dataTransferMessage.getECSMessage());
    }
  }

  private synchronized KVMessage getKV(final KVMessage request) throws NoSuchElementException {
    if (!checkNodeResponsibleForRequest(request)) {
      logger.info(
          "Node not responsible for request with key: "
              + request.getKey()
              + " hash: "
              + Hashing.calculateMD5Hash(request.getKey()));
      return new KVMessage(
          request.getKey(),
          request.getValue(),
          request.getClientId(),
          KVMessage.StatusType.NOT_RESPONSIBLE,
          request.getRequestId());
    }
    try {
      return new KVMessage(
          request.getKey(),
          cache.get(request.getKey()),
          request.getClientId(),
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
            key, Hashing.calculateMD5Hash(key), Arrays.toString(identityNode.getNodeHashRange())));
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

  private synchronized KVMessage writeKV(final KVMessage request) {
    if (!checkNodeResponsibleForRequest(request)) {
      logger.info(
          "Node not responsible for request with key: "
              + request.getKey()
              + " hash: "
              + Hashing.calculateMD5Hash(request.getKey()));
      return new KVMessage(
          request.getKey(),
          request.getValue(),
          request.getClientId(),
          KVMessage.StatusType.NOT_RESPONSIBLE,
          request.getRequestId());
    }
    if (!writingIsAvailable()) {
      logger.info("Writing is not available to serve request: " + request.toString());
      return new KVMessage(
          request.getKey(),
          request.getValue(),
          request.getClientId(),
          KVMessage.StatusType.SERVER_WRITE_LOCK,
          request.getRequestId());
    }

    // caching
    if (request.getStatus() == PUT) {
      if (request.getValue() == null) {
        logger.info("Deleting key from cache");
        cache.remove(request.getKey());
      } else {
        logger.info("Adding key to cache");
        cache.put(request.getKey(), request.getValue());
      }
      logger.info("Accessing disk storage for key");
    }

    DiskStorage.DiskStorageWriteResponse response =
        diskStorage.write(request, DiskStorage.StorageType.SELF);
    KVMessage kvMessageResponse = response.getKvMessageResponse();

    // Notify subscribers if data changed
    if (kvMessageResponse.getStatus() == PUT_SUCCESS
        || kvMessageResponse.getStatus() == PUT_UPDATE
        || kvMessageResponse.getStatus() == DELETE_SUCCESS) {
      logger.info("Notifying subscribers of change: " + response.getSubscribers());
      new Thread(
              () -> {
                ArrayList<UUID> toRemove = new ArrayList<>();
                response
                    .getSubscribers()
                    .forEach(
                        subscriberId -> {
                          KVServerConnection connection = connections.get(subscriberId);
                          if (connection == null) {
                            logger.info(
                                "Unable to find connection for: "
                                    + subscriberId
                                    + " | connections: "
                                    + connections.keySet());
                            toRemove.add(subscriberId);
                          } else {
                            logger.info("found connection for: " + subscriberId);
                            if (connection.isRunning()) {
                              connection.notifyClient(
                                  new KVMessage(
                                      kvMessageResponse.getKey(),
                                      kvMessageResponse.getValue(),
                                      kvMessageResponse.getClientId(),
                                      NOTIFY,
                                      new UUID(0, 0)));
                              logger.info("Notified client for: " + subscriberId);
                            } else {
                              logger.info(
                                  "reference to connection to client "
                                      + subscriberId
                                      + " stale. Removing.");
                              KVServerConnection staleConnection = connections.remove(subscriberId);

                              // Handle race condition
                              if (staleConnection != connection && staleConnection != null) {
                                // connection was replaced concurrently and then we mistakenly
                                // removed the replacement.
                                KVServerConnection mistakenlyReplacedConnection = staleConnection;
                                while (true) {
                                  // Put the mistakenly replaced connection back and capture what
                                  // taht replaces
                                  KVServerConnection replacedConnection =
                                      connections.put(subscriberId, mistakenlyReplacedConnection);
                                  if (replacedConnection == null
                                      || replacedConnection == mistakenlyReplacedConnection) {
                                    // Putting the mistakenlyReplacedConnection back replaced
                                    // nothing or itself, which is a consistent state
                                    break;
                                  }
                                  // There was another concurrent replacement (rare) so we need to
                                  // put that one back now
                                  mistakenlyReplacedConnection = replacedConnection;
                                }
                              }
                            }
                          }
                        });
                //                toRemove.forEach(
                //                    subscriberId ->
                //                        this.writeKV(
                //                            new KVMessage(
                //                                request.getKey(),
                //                                null,
                //                                subscriberId,
                //                                UNSUBSCRIBE,
                //                                new UUID(0, 0))) // todo replication
                //                    );
              })
          .start();
    }

    return kvMessageResponse;
  }

  public synchronized boolean moveReplicaDataToSelfStorage(
      ECSMessage ecsMessage, String[] hashRange) {
    HashSet<StorageUnit> replica1Data =
        diskStorage
            .partitionDatabaseAndGetKeysInRange(
                ecsMessage, hashRange, DiskStorage.StorageType.REPLICA_1, true)
            .getPayload();
    HashSet<StorageUnit> replica2Data =
        diskStorage
            .partitionDatabaseAndGetKeysInRange(
                ecsMessage, hashRange, DiskStorage.StorageType.REPLICA_2, true)
            .getPayload();
    boolean success1 = moveReplicaDataToSelf(replica1Data);
    boolean success2 = moveReplicaDataToSelf(replica2Data);
    return success1 && success2;
  }

  private boolean moveReplicaDataToSelf(HashSet<StorageUnit> replica1Data) {
    boolean success = true;
    for (StorageUnit entry : replica1Data) {
      KVMessage.StatusType response =
          diskStorage.putStorageUnit(entry, DiskStorage.StorageType.SELF);
      success = success && (response == PUT_SUCCESS || response == PUT_UPDATE);
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
