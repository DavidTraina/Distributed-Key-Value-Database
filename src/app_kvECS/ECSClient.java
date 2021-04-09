package app_kvECS;

import static ecs.ECSUtils.sendECSMessageToNode;

import app_kvServer.data.cache.CacheStrategy;
import com.google.common.collect.Sets;
import ecs.ECSMetadata;
import ecs.ECSNode;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.spec.InvalidKeySpecException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import shared.communication.messages.ECSMessage;
import shared.communication.security.KeyLoader;
import shared.communication.security.property_stores.ECSPropertyStore;

public class ECSClient implements Runnable {
  private static final Logger logger = Logger.getLogger(ECSClient.class);
  private final String SERVER_SSH_COMMAND;
  private final CacheStrategy cacheStrategy;
  private final int cacheSize;
  public ArrayList<ECSNode> availableNodes;
  public Hashtable<String, ECSNode> allNodes;
  private ZKManager zkManager;
  private CountDownLatch awaitNodesEvents;
  private final HashSet<String> expectedZookeeperNodeEvent = new HashSet<>();
  private HashSet<String> existingNodesSet = new HashSet<>();
  private volatile ArrayList<String> crashedNodesToAddBack = new ArrayList<>();
  private boolean watchingCrashedNodes = true;

  public ECSClient(
      ArrayList<ECSNode> availableNodes,
      int numberOfNodes,
      String zkAddress,
      int zkPort,
      CacheStrategy cacheStrategy,
      int cacheSize,
      boolean encrypted) {
    CLIECSUtils.printMessage("Initializing Nodes...");
    logger.info("Starting ECS");
    this.availableNodes = availableNodes;
    this.cacheStrategy = cacheStrategy;
    this.cacheSize = cacheSize;
    String projectPath = System.getProperty("user.dir");
    this.SERVER_SSH_COMMAND =
        new StringBuilder()
            .append("ssh -n %s cd ")
            .append(projectPath)
            .append(" && nohup java -jar m2-server.jar %s ")
            .append(this.cacheSize)
            .append(" ")
            .append(this.cacheStrategy.toString())
            .append(" ")
            .append(zkAddress)
            .append(" ")
            .append(zkPort)
            .append(" ")
            .append(encrypted)
            .append(" %s")
            .append(" &> /dev/null &")
            .toString();
    this.allNodes = createAllNodesLookup(availableNodes);
    initializePrivateKey();
    ECSPropertyStore.getInstance().setSenderID("ecs");
    initialize(numberOfNodes, zkAddress, zkPort);
  }

  public ECSClient(
      ArrayList<ECSNode> availableNodes,
      int numberOfNodes,
      String zkAddress,
      int zkPort,
      boolean encrypted) {
    logger.info("Starting ECS");
    this.availableNodes = availableNodes;
    this.cacheStrategy = null;
    this.cacheSize = 0;
    String projectPath = System.getProperty("user.dir");
    this.SERVER_SSH_COMMAND =
        new StringBuilder()
            .append("ssh -n %s cd ")
            .append(projectPath)
            .append(" && nohup java -jar m2-server.jar %s 0 LRU ")
            .append(zkAddress)
            .append(" ")
            .append(zkPort)
            .append(" ")
            .append(encrypted)
            .append(" %s &> /dev/null &")
            .toString();
    this.allNodes = createAllNodesLookup(availableNodes);
    initializePrivateKey();
    ECSPropertyStore.getInstance().setSenderID("ecs");
    initialize(numberOfNodes, zkAddress, zkPort);
  }

  private Hashtable<String, ECSNode> createAllNodesLookup(ArrayList<ECSNode> availableNodes) {
    Hashtable<String, ECSNode> nodesTable = new Hashtable<>();
    for (ECSNode node : availableNodes) {
      nodesTable.put(node.getNodeName(), node);
    }
    return nodesTable;
  }

  public static void initializePrivateKey() {
    try {
      // Set Private Key For KV Store
      ECSPropertyStore.getInstance()
          .setPrivateKey(KeyLoader.getPrivateKey(ECSPrivateKey.base64EncodedPrivateKey));
    } catch (InvalidKeySpecException e) {
      logger.error("Client private key is invalid");
      e.printStackTrace();
    }
  }

  private void initialize(int numberOfNodes, String zkAddress, int zkPort) {
    ECSMetadata.initialize(new ArrayList<>());
    setupZookeeper(zkAddress, zkPort);
    addNodes(numberOfNodes);
    CLIECSUtils.printMessage("Initialization complete");
  }

  private void startServerProcess(ECSNode node) {
    String command =
        String.format(
            SERVER_SSH_COMMAND, node.getNodeHost(), node.getNodePort(), node.getNodeName());
    try {
      logger.info("Starting server process with command: " + command);
      Runtime.getRuntime().exec(command);
    } catch (IOException e) {
      logger.error("Error starting " + node.toString(), e);
      e.printStackTrace();
    }
  }

  private void setupZookeeper(String zkAddress, int zkPort) {
    zkManager = new ZKManager(zkAddress, zkPort);
    zkManager.delete("/metadata");
    for (String zNode : zkManager.getChildrenList("/nodes", false)) {
      zkManager.delete("/nodes/" + zNode);
    }

    zkManager.create(
        "/metadata",
        new ECSMessage(ECSMessage.ActionType.UPDATE_METADATA, ECSMetadata.getInstance())
            .calculateAndSetMAC()
            .serialize());
    zkManager.create("/nodes", "KV Servers".getBytes(StandardCharsets.UTF_8));

    zkManager.getChildrenList(
        "/nodes",
        new Watcher() {
          @Override
          public void process(WatchedEvent event) {
            if (event.getType() == Event.EventType.NodeChildrenChanged) {
              List<String> allNodes = zkManager.getChildrenList("/nodes", this);
              handleZKNodeEvent(allNodes);
            }
          }
        });
  }

  /**
   * Method handles zookeeper /node events i.e when a node is added or removed The method figures
   * out if a node was deleted or added. Then checks whether this change was expected. If yes, then
   * update metadata. If no, then log error and update metadata
   *
   * @param currentNodes list of nodes from zookeeper
   */
  private synchronized void handleZKNodeEvent(List<String> currentNodes) {
    logger.info("Cluster Node Changed, Here are all nodes -> " + currentNodes.toString());

    HashSet<String> currentNodesSet = new HashSet<>(currentNodes);
    Set<String> commonElements = Sets.intersection(currentNodesSet, existingNodesSet);
    Set<String> addedNode = Sets.difference(currentNodesSet, commonElements);
    boolean expectedEvent = true;
    String removedNodeName = null;

    // Case when node deleted from cluster
    if (addedNode.size() == 0) {
      Set<String> removedNode = Sets.difference(existingNodesSet, commonElements);
      removedNodeName = removedNode.stream().findFirst().get();
      if (expectedZookeeperNodeEvent.remove(removedNodeName)) {
        logger.info("Node removed by ECS: " + removedNodeName);
      } else {
        logger.info("Node crashed: " + removedNodeName);
        CLIECSUtils.printError("Node crashed: " + removedNodeName);
        expectedEvent = false;
        handleNodeCrash(removedNodeName);
      }
    } else {
      String addedNodeName = addedNode.stream().findFirst().get();
      if (expectedZookeeperNodeEvent.remove(addedNodeName)) {
        logger.info("New node added by ECS: " + addedNodeName);
      } else {
        logger.error(
            String.format(
                "Node %s added to cluster but not by ECS, this should NOT happen", addedNodeName));
      }
    }

    existingNodesSet = currentNodesSet;
    zkManager.update(
        "/metadata",
        new ECSMessage(ECSMessage.ActionType.UPDATE_METADATA, ECSMetadata.getInstance())
            .calculateAndSetMAC()
            .serialize());

    // Very basic way of propagating events
    if (expectedEvent) {
      awaitNodesEvents.countDown();
    } else {
      crashedNodesToAddBack.add(removedNodeName);
      logger.info("Crashed node added to list, current size: " + crashedNodesToAddBack.size());
    }
  }

  private void handleNodeCrash(String nodeName) {
    ECSMetadata.getInstance().removeNodeFromTheRing(nodeName);
  }

  private ECSNode chooseARandomNode() {
    Random randomGenerator = new Random();
    int index = randomGenerator.nextInt(availableNodes.size());
    ECSNode chosenNode = availableNodes.get(index);
    availableNodes.remove(index);
    return chosenNode;
  }

  public boolean start() {
    boolean endResult = true;
    for (ECSNode node : ECSMetadata.getInstance().getNodeRing()) {
      boolean result =
          sendECSMessageToNode(
              node, new ECSMessage(ECSMessage.ActionType.START).calculateAndSetMAC());
      endResult = result && endResult;
    }
    return endResult;
  }

  public boolean stop() {
    boolean endResult = true;
    for (ECSNode node : ECSMetadata.getInstance().getNodeRing()) {
      boolean result =
          sendECSMessageToNode(
              node, new ECSMessage(ECSMessage.ActionType.STOP).calculateAndSetMAC());
      endResult = result && endResult;
    }
    return endResult;
  }

  public boolean shutdown() {
    boolean endResult = true;
    ArrayList<ECSNode> nodesToKill = new ArrayList<>(ECSMetadata.getInstance().getNodeRing());
    for (ECSNode node : nodesToKill) {
      boolean result = removeNode(node.getNodeName());
      endResult = result && endResult;
    }
    return endResult;
  }

  public void shutDownECS() {
    zkManager.closeConnection();
    watchingCrashedNodes = false;
  }

  public ECSMetadata getMetadata() {
    return ECSMetadata.getInstance();
  }

  public ECSNode addNode() {
    if (this.availableNodes.size() == 0) {
      CLIECSUtils.printError(
          "Number of nodes has reached its limit. Provide more nodes in the config file and"
              + " restart ECS.");
      return null;
    }
    ECSNode nodeToAdd = chooseARandomNode();
    return addSpecificNode(nodeToAdd);
  }

  public ECSNode addSpecificNode(ECSNode nodeToAdd) {
    ECSNode[] affectedNodes = ECSMetadata.getInstance().placeNewNodeOnTheRing(nodeToAdd);

    // Let zookeeper know that there might be an event from this server
    expectedZookeeperNodeEvent.add(nodeToAdd.getNodeName());
    startServerProcess(nodeToAdd);

    if (awaitNodes(1, 30)) {
      CLIECSUtils.printMessage(
          "Started Node on " + nodeToAdd.getNodeHost() + ":" + nodeToAdd.getNodePort());
      logger.info("Node started successfully");
    } else {
      // Revert changes made
      ECSMetadata.getInstance().removeNodeFromTheRing(nodeToAdd.getNodeName());
      this.availableNodes.add(nodeToAdd);
      CLIECSUtils.printError("Node not added properly, try again");
      logger.error("Server process did not start successfully, awaitNode failed");
      return null;
    }

    if (affectedNodes.length == 2) {
      sendECSMessageToNode(
          affectedNodes[1], new ECSMessage(ECSMessage.ActionType.LOCK_WRITE).calculateAndSetMAC());
    }

    if (affectedNodes.length == 2) {
      ECSMessage moveData =
          new ECSMessage(ECSMessage.ActionType.MOVE_DATA, nodeToAdd, nodeToAdd.getNodeHashRange());

      sendECSMessageToNode(affectedNodes[1], moveData.calculateAndSetMAC());
      sendECSMessageToNode(
          affectedNodes[1],
          new ECSMessage(ECSMessage.ActionType.UNLOCK_WRITE).calculateAndSetMAC());
    }

    return nodeToAdd;
  }

  public Collection<ECSNode> addNodes(int count) {
    ArrayList<ECSNode> nodesToAdd = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      ECSNode nodeToAdd = addNode();
      try {
        // Sleep when consecutively launching nodes
        TimeUnit.SECONDS.sleep(1);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      if (nodeToAdd == null) {
        return null;
      }
      nodesToAdd.add(nodeToAdd);
    }
    return nodesToAdd;
  }

  public boolean removeNode(String nodeName) {
    ECSNode[] affectedNodes = ECSMetadata.getInstance().removeNodeFromTheRing(nodeName);
    if (affectedNodes == null) {
      logger.error(nodeName + " is not a valid node to remove");
      CLIECSUtils.printError(nodeName + " is not a valid node to remove");
      return false;
    }
    ECSNode nodeToRemove = affectedNodes[0];

    // Let zookeeper know that there might be an event from this server
    sendECSMessageToNode(
        nodeToRemove, new ECSMessage(ECSMessage.ActionType.STOP).calculateAndSetMAC());

    if (affectedNodes.length == 2) {
      ECSNode affectedNode = affectedNodes[1];
      sendECSMessageToNode(
          affectedNode, new ECSMessage(ECSMessage.ActionType.LOCK_WRITE).calculateAndSetMAC());
      ECSMessage moveData =
          new ECSMessage(
                  ECSMessage.ActionType.MOVE_DATA, affectedNode, affectedNode.getNodeHashRange())
              .calculateAndSetMAC();
      sendECSMessageToNode(nodeToRemove, moveData.calculateAndSetMAC());
    }

    if (affectedNodes.length == 2) {
      ECSNode affectedNode = affectedNodes[1];
      sendECSMessageToNode(
          affectedNode, new ECSMessage(ECSMessage.ActionType.UNLOCK_WRITE).calculateAndSetMAC());
    }

    expectedZookeeperNodeEvent.add(nodeToRemove.getNodeName());
    sendECSMessageToNode(
        nodeToRemove, new ECSMessage(ECSMessage.ActionType.SHUTDOWN).calculateAndSetMAC());

    if (awaitNodes(1, 30)) {
      logger.info("Node removed successfully");
      this.availableNodes.add(nodeToRemove);
    } else {
      logger.error("Await nodes failed due to timeout");
      CLIECSUtils.printError(String.format("ERROR: Node %s not removed, try again", nodeName));
      return false;
    }

    return true;
  }

  public boolean awaitNodes(int count, int timeout) {
    this.awaitNodesEvents = new CountDownLatch(count);
    try {
      return this.awaitNodesEvents.await(timeout, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      logger.error("Error waiting for nodes in awaitNodes due to: " + e);
    }
    return false;
  }

  @Override
  public void run() {
    while (watchingCrashedNodes) {
      if (crashedNodesToAddBack.size() > 0) {
        try {
          // Wait for node crash recovery actions to take place
          TimeUnit.SECONDS.sleep(5);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        String nodeToRevive = crashedNodesToAddBack.remove(0);
        logger.info("Reviving crashed node: " + nodeToRevive);
        addSpecificNode(allNodes.get(nodeToRevive));
        start(); // Start all nodes, including revived one
        logger.info("Done reviving crashed node: " + nodeToRevive);
      }
    }
  }
}
