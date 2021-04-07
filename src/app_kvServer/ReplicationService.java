package app_kvServer;

import static shared.communication.messages.DataTransferMessage.DataTransferMessageType.*;

import app_kvServer.data.SynchronizedKVManager;
import app_kvServer.data.storage.DiskStorage;
import com.google.common.collect.Sets;
import ecs.ECSMetadata;
import ecs.ECSMetadataUtils;
import ecs.ECSNode;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.log4j.Logger;
import shared.communication.Protocol;
import shared.communication.ProtocolException;
import shared.communication.messages.DataTransferMessage;
import shared.communication.messages.KVMessage;
import shared.communication.messages.Message;
import shared.communication.messages.ReplicationMessage;

// TODO: Refactor file, pretty messy at the moment
public class ReplicationService implements Runnable {

  private static final Logger logger = Logger.getLogger(ReplicationService.class);
  private final String nodeName;
  LinkedBlockingQueue<KVMessage> replicationQueue;
  HashMap<String, Socket> socketCache = new HashMap<>();

  private final Phaser pauseReplicationService = new Phaser(1);

  public ReplicationService(
      final LinkedBlockingQueue<KVMessage> replicationQueue, final String nodeName) {
    this.replicationQueue = replicationQueue;
    this.nodeName = nodeName;
  }

  @Override
  public void run() {
    logger.info("Replication service started");
    while (true) {
      KVMessage message = null;
      ReplicationMessage replicationMessage = null;
      try {
        message = replicationQueue.take();
        replicationMessage = new ReplicationMessage(message);
        pauseReplicationService.arriveAndAwaitAdvance();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      assert message != null;
      logger.info(
          "Sending message to replicas with key: "
              + message.getKey()
              + " queue length: "
              + replicationQueue.size());
      ECSNode[] replicas = ECSMetadata.getInstance().getReplicasBasedOnName(this.nodeName);

      for (ECSNode replica : replicas) {
        KVMessage response = (KVMessage) sendMessageToServer(replica, replicationMessage, true);
        assert response != null;
        if (response.getStatus() == KVMessage.StatusType.FAILED
            || response.getStatus() == KVMessage.StatusType.PUT_ERROR
            || response.getStatus() == KVMessage.StatusType.DELETE_ERROR) {
          logger.error(
              "Error replicating request: "
                  + message.toString()
                  + " to replica: "
                  + replica.getNodeName());
        }
      }
    }
  }

  public synchronized void handleMetadataChange(
      ArrayList<ECSNode> oldMetadata, ArrayList<ECSNode> newMetadata) {
    if (oldMetadata.size() == 0 && newMetadata.size() == 0) {
      return;
    }
    logger.info("Pausing replication service, current queue length " + replicationQueue.size());
    pauseReplicationService.register();
    try {
      logger.info("Metadata Change Detected, Replication service paused and taking action");
      ECSNode[] oldReplicas = ECSMetadataUtils.getReplicasBasedOnName(this.nodeName, oldMetadata);
      ECSNode[] newReplicas = ECSMetadataUtils.getReplicasBasedOnName(this.nodeName, newMetadata);

      HashSet<String> currentNodesSet =
          newMetadata.stream()
              .map(ECSNode::getNodeName)
              .collect(Collectors.toCollection(HashSet::new));
      HashSet<String> oldNodesSet =
          oldMetadata.stream()
              .map(ECSNode::getNodeName)
              .collect(Collectors.toCollection(HashSet::new));
      Set<String> commonElements = Sets.intersection(currentNodesSet, oldNodesSet);

      boolean nodeAdded = newMetadata.size() > oldMetadata.size();
      if (nodeAdded) {
        Set<String> addedNodeSet = Sets.difference(currentNodesSet, commonElements);
        String addedNodeName = addedNodeSet.stream().findFirst().get();
        ECSNode addedNode = ECSMetadataUtils.getNodeBasedOnName(addedNodeName, newMetadata);
        logger.info("New node added: " + addedNodeName);
        handleNewNode(newMetadata, oldReplicas, newReplicas, addedNodeName, addedNode);
        logger.info("Done handling new node event");

      } else {
        Set<String> removedNodeSet = Sets.difference(oldNodesSet, commonElements);
        String removedNodeName = removedNodeSet.stream().findFirst().get();
        ECSNode removedNode = ECSMetadataUtils.getNodeBasedOnName(removedNodeName, oldMetadata);
        logger.info("Node removal detected: " + removedNodeName);
        handleNodeRemoval(
            oldMetadata, newMetadata, oldReplicas, newReplicas, removedNodeName, removedNode);
        logger.info("Done handling node removal event");
      }
    } finally {
      pauseReplicationService.arriveAndDeregister();
      logger.info(
          "Continuing replication service, current queue length " + replicationQueue.size());
    }
  }

  private void handleNewNode(
      ArrayList<ECSNode> newMetadata,
      ECSNode[] oldReplicas,
      ECSNode[] newReplicas,
      String addedNodeName,
      ECSNode addedNode) {
    ECSNode currentNode = ECSMetadataUtils.getNodeBasedOnName(this.nodeName, newMetadata);
    if (oldReplicas != null)
      logger.info(
          "Old replicas: "
              + Arrays.stream(oldReplicas)
                  .map(ECSNode::getNodeName)
                  .collect(Collectors.toList())
                  .toString());
    if (newReplicas != null)
      logger.info(
          "New replicas: "
              + Arrays.stream(newReplicas)
                  .map(ECSNode::getNodeName)
                  .collect(Collectors.toList())
                  .toString());

    assert currentNode != null;
    if (addedNodeName.equals(currentNode.getNodeName())) {
      logger.info("Added node is the current node");
      // TODO: Remove hack: sleeping to ensure current node gets data from adjacent node
      // Maybe check for this data in local storage before replication or ask adjacent node
      // for our own keys and wait till it has none
      try {
        TimeUnit.SECONDS.sleep(2);
      } catch (InterruptedException e) {
        logger.error("Interrupted when sleeping");
      }
      handleReplicationToNewReplicas(oldReplicas, newReplicas, currentNode, 1);
      handleReplicationToNewReplicas(oldReplicas, newReplicas, currentNode, 2);
    } else if (addedNodeName.equals(
        Objects.requireNonNull(ECSMetadataUtils.findPredecessor(this.nodeName, newMetadata))
            .getNodeName())) {
      logger.info("Added node is a predecessor of current node");
      // Ask old replicas to delete new node's hash range from their replicas (new node will send
      // them replica data)
      for (int i = 0; i < Objects.requireNonNull(oldReplicas).length; ++i) {
        DiskStorage.StorageType storageType =
            i == 1 ? DiskStorage.StorageType.REPLICA_1 : DiskStorage.StorageType.REPLICA_2;
        deleteReplicaDataFromNode(addedNode.getNodeHashRange(), storageType, oldReplicas[i]);
      }

      // Initialize any new replicas with own data if applicable
      handleReplicationToNewReplicas(oldReplicas, newReplicas, currentNode, 1);
      handleReplicationToNewReplicas(oldReplicas, newReplicas, currentNode, 2);

    } else if (addedNodeName.equals(
        Objects.requireNonNull(ECSMetadataUtils.findSuccessor(this.nodeName, newMetadata))
            .getNodeName())) {
      // Added node is a successor
      logger.info("Added node is a successor of the current node");

      // Ask old replica 1 to move files r1 -> r2
      assert oldReplicas != null;
      if (oldReplicas.length > 0) {
        askNodeToSwitchReplicaFiles(MOVE_REPLICA1_TO_REPLICA2, currentNode, oldReplicas[0]);
      }

      // Ask old replica 2 to delete data as it is not a replica anymore
      if (oldReplicas.length == 2) {
        deleteReplicaDataFromNode(
            currentNode.getNodeHashRange(), DiskStorage.StorageType.REPLICA_2, oldReplicas[1]);
      }

      // Initialize any new nodes with replicas
      handleReplicationToNewReplicas(oldReplicas, newReplicas, currentNode, 1);
      handleReplicationToNewReplicas(oldReplicas, newReplicas, currentNode, 2);

    } else {
      logger.info("Node addition detected, no special case needed for current node");
      // If new node is new replica 2, then delete current node's replica from old replica2
      assert newReplicas != null;
      if (newReplicas.length == 2 && addedNode.getNodeName().equals(newReplicas[1].getNodeName())) {
        assert oldReplicas != null;
        if (oldReplicas.length == 2) {
          logger.info("Added node is new replica 2, asking old replica 2 to delete data");
          deleteReplicaDataFromNode(
              currentNode.getNodeHashRange(), DiskStorage.StorageType.REPLICA_2, oldReplicas[1]);
        }
      }

      // Initialize any new replicas
      handleReplicationToNewReplicas(oldReplicas, newReplicas, currentNode, 1);
      handleReplicationToNewReplicas(oldReplicas, newReplicas, currentNode, 2);
    }
  }

  private void handleNodeRemoval(
      ArrayList<ECSNode> oldMetadata,
      ArrayList<ECSNode> newMetadata,
      ECSNode[] oldReplicas,
      ECSNode[] newReplicas,
      String removedNodeName,
      ECSNode removedNode) {
    ECSNode currentNode = ECSMetadataUtils.getNodeBasedOnName(this.nodeName, newMetadata);
    assert currentNode != null;
    if (removedNodeName.equals(
        Objects.requireNonNull(ECSMetadataUtils.findPredecessor(this.nodeName, oldMetadata))
            .getNodeName())) {
      logger.info("Removed node is a predecessor of current node");
      // Removed node is a predecessor
      // 1. Ensure X's replica data in storage
      // 2. Delete X's replica data using hash range
      // (both done in same step)
      SynchronizedKVManager.getInstance()
          .moveReplicaDataToSelfStorage(removedNode.getNodeHashRange());
      logger.info("Ensured removed node's data is in SELF storage");

      // 3. If new replica 2, then update with X's data (part of current storage)
      if (newReplicas.length == 2) {
        logger.info("Updating replica2 with removed node's data");
        transferReplicaDataToNode(removedNode, DiskStorage.StorageType.REPLICA_2, newReplicas, 1);
        logger.info("Done updating replica2 with removed node's data");
        askNodeToSwitchReplicaFiles(MOVE_REPLICA2_TO_REPLICA1, currentNode, newReplicas[0]);
        logger.info(
            "Successfully asked replica1: "
                + newReplicas[0].getNodeName()
                + " to move replica2 file to replica1");
      }
      if (newReplicas.length == 1) {
        askNodeToSwitchReplicaFiles(MOVE_REPLICA2_TO_REPLICA1, currentNode, newReplicas[0]);
        logger.info(
            "Successfully asked replica1: "
                + newReplicas[0].getNodeName()
                + " to move replica2 file to replica1");
      }
    } else if (removedNodeName.equals(
        Objects.requireNonNull(ECSMetadataUtils.findSuccessor(this.nodeName, oldMetadata))
            .getNodeName())) {
      logger.info("Removed node is a successor of the current node");
      // Removed node is a successor
      if (oldReplicas.length == 2) {
        assert oldReplicas[1].getNodeName().equals(newReplicas[0].getNodeName());
        // Ask old replica to switch files from r2 -> r1, for all keys in range
        askNodeToSwitchReplicaFiles(MOVE_REPLICA2_TO_REPLICA1, currentNode, newReplicas[0]);
        logger.info(
            "Successfully asked replica1: "
                + newReplicas[0].getNodeName()
                + " to move replica2 file to replica1");
        handleReplicationToNewReplicas(oldReplicas, newReplicas, currentNode, 2);
        logger.info("Replicated to replica2 if exists");
      }

    } else {
      handleReplicationToNewReplicas(oldReplicas, newReplicas, currentNode, 1);
      handleReplicationToNewReplicas(oldReplicas, newReplicas, currentNode, 2);
    }
  }

  private void askNodeToSwitchReplicaFiles(
      DataTransferMessage.DataTransferMessageType direction,
      ECSNode currentNode,
      ECSNode nodeToRequest) {
    String message =
        "Node "
            + currentNode.getNodeName()
            + " asking node "
            + nodeToRequest.getNodeName()
            + " to switch replicas "
            + direction.name()
            + " with all keys in range: "
            + Arrays.toString(currentNode.getNodeHashRange());
    logger.info(message);
    DataTransferMessage moveMessage =
        new DataTransferMessage(direction, currentNode.getNodeHashRange(), message);
    DataTransferMessage response =
        (DataTransferMessage) sendMessageToServer(nodeToRequest, moveMessage, false);
    assert response != null;
    if (response.getDataTransferMessageType() != DATA_TRANSFER_SUCCESS) {
      logger.error(
          String.format(
              "Request %s to node %s failed!", direction.name(), nodeToRequest.getNodeName()));
    }
  }

  private void handleReplicationToNewReplicas(
      ECSNode[] oldReplicas, ECSNode[] newReplicas, ECSNode currentNode, int replicaNumber) {
    DiskStorage.StorageType storageType =
        replicaNumber == 1 ? DiskStorage.StorageType.REPLICA_1 : DiskStorage.StorageType.REPLICA_2;
    if (checkIfNewReplica(oldReplicas, newReplicas, replicaNumber)) {
      logger.info(
          "Starting replication for node: "
              + currentNode.getNodeName()
              + " to node: "
              + newReplicas[replicaNumber - 1].getNodeName());
      transferReplicaDataToNode(currentNode, storageType, newReplicas, replicaNumber - 1);
    }
  }

  private void transferReplicaDataToNode(
      ECSNode currentNode, DiskStorage.StorageType storageType, ECSNode[] newReplicas, int i) {
    DataTransferMessage dtmsg =
        SynchronizedKVManager.getInstance()
            .getDataChunkForReplication(currentNode.getNodeHashRange());
    dtmsg.setStorageType(storageType);
    DataTransferMessage response =
        (DataTransferMessage) sendMessageToServer(newReplicas[i], dtmsg, false);
    assert response != null;
    if (response.getDataTransferMessageType() == DATA_TRANSFER_FAILURE) {
      logger.error("Data transfer to replica failed ->" + newReplicas[i].getNodeName());
    } else {
      logger.info("Replication Successful!!");
    }
  }

  private void deleteReplicaDataFromNode(
      String[] hashRange, DiskStorage.StorageType storageType, ECSNode nodeToAsk) {
    DataTransferMessage dtmsg =
        new DataTransferMessage(
            DELETE_DATA,
            hashRange,
            String.format(
                "Node %s asking node %s to delete replica %s with data in range %s",
                this.nodeName,
                nodeToAsk.getNodeName(),
                storageType.name(),
                Arrays.toString(hashRange)));
    dtmsg.setStorageType(storageType);
    DataTransferMessage response =
        (DataTransferMessage) sendMessageToServer(nodeToAsk, dtmsg, false);
    assert response != null;
    if (response.getDataTransferMessageType() == DATA_TRANSFER_FAILURE) {
      logger.error(
          "Deletion request to replica failed ->"
              + nodeToAsk.getNodeName()
              + " message: "
              + response.getMessage());
    } else {
      logger.info("Deletion Successful!!");
    }
  }

  private boolean checkIfNewReplica(
      ECSNode[] oldReplicas, ECSNode[] newReplicas, int replicaNumber) {
    // Case when new node added
    if (oldReplicas == null) {
      return newReplicas.length >= replicaNumber;
    }
    if (replicaNumber == 1) {
      if (newReplicas.length > 0 && oldReplicas.length > 0) {
        return !(newReplicas[0].getNodeName().equals(oldReplicas[0].getNodeName()));
      } else return newReplicas.length >= 1;
    } else if (replicaNumber == 2) {
      if (newReplicas.length == 2 && oldReplicas.length == 2) {
        return !(newReplicas[1].getNodeName().equals(oldReplicas[1].getNodeName()));
      } else if (newReplicas.length == 2 && oldReplicas.length == 1) {
        return !(oldReplicas[0].getNodeName().equals(newReplicas[1].getNodeName()));
      } else {
        return newReplicas.length == 2;
      }
    }
    return false;
  }

  private Message sendMessageToServer(ECSNode node, Message message, boolean cacheConnection) {
    Socket nodeSocket;
    if (!cacheConnection) {
      socketCache.remove(node.getNodeName());
    }
    if (socketCache.containsKey(node.getNodeName())) {
      nodeSocket = socketCache.get(node.getNodeName());
    } else {
      try {
        nodeSocket = new Socket(InetAddress.getByName(node.getNodeHost()), node.getNodePort());
        nodeSocket.setSoTimeout(10000);
        if (cacheConnection) {
          socketCache.put(node.getNodeName(), nodeSocket);
        }
      } catch (IOException e) {
        e.printStackTrace();
        return null;
      }
    }
    try {
      OutputStream outputStream = nodeSocket.getOutputStream();
      InputStream inputStream = nodeSocket.getInputStream();

      Protocol.sendMessage(outputStream, message);
      return Protocol.receiveMessage(inputStream);
    } catch (SocketTimeoutException e) {
      logger.error("Did not receive reply from server in 10s");
      e.printStackTrace();
    } catch (IOException e) {
      // TODO: Handle a stale socket, will reach here
      logger.error("Socket problems");
      e.printStackTrace();
    } catch (ProtocolException e) {
      logger.error("Problem receiving message from node");
      e.printStackTrace();
    }
    return null;
  }
}
