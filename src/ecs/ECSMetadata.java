package ecs;

import java.util.ArrayList;
import java.util.stream.Collectors;
import org.apache.log4j.Logger;

public class ECSMetadata {

  private static final Logger logger = Logger.getLogger(ECSMetadata.class);

  private static ECSMetadata singletonECSMetadata = null;

  private ArrayList<ECSNode> ring;

  private ECSMetadata() {}

  public static void initialize(ArrayList<ECSNode> serverData) {
    if (singletonECSMetadata == null) {
      singletonECSMetadata = new ECSMetadata();
    }
    singletonECSMetadata.ring = serverData;
  }

  public static ECSMetadata getInstance() {
    assert (singletonECSMetadata != null);
    return singletonECSMetadata;
  }

  public int getNumberOfNodes() {
    return ring.size();
  }

  public void clear() {
    this.ring.clear();
  }

  public void update(ECSMetadata newMetadata) {
    singletonECSMetadata.ring = newMetadata.getNodeRing();
  }

  public void updateArray(ArrayList<ECSNode> serverData) {
    singletonECSMetadata.ring = serverData;
  }

  public ArrayList<ECSNode> getNodeRing() {
    return ring;
  }

  public ECSNode findPredecessor(String nodeName) {
    return ECSMetadataUtils.findPredecessor(nodeName, ring);
  }

  public ECSNode findSuccessor(String nodeName) {
    return ECSMetadataUtils.findSuccessor(nodeName, ring);
  }

  public ECSNode getNodeBasedOnKey(String key) {
    return ECSMetadataUtils.getNodeBasedOnKey(key, ring);
  }

  public ECSNode getNodeBasedOnName(String name) {
    return ECSMetadataUtils.getNodeBasedOnName(name, ring);
  }

  public ECSNode[] getReplicasBasedOnName(String name) {
    return ECSMetadataUtils.getReplicasBasedOnName(name, ring);
  }

  public ECSNode[] getNodesWhereIAmReplicaBasedOnName(String name) {
    return ECSMetadataUtils.getNodesWhereIAmReplicaBasedOnName(name, ring);
  }

  public ECSNode[] placeNewNodeOnTheRing(ECSNode newNode) {
    return ECSMetadataUtils.placeNewNodeOnTheRing(newNode, ring);
  }

  public ECSNode[] removeNodeFromTheRing(String nodeName) {
    return ECSMetadataUtils.removeNodeFromTheRing(nodeName, ring);
  }

  @Override
  public String toString() {
    return "ECSMetadata( numNodes="
        + ring.size()
        + ", nodeRing="
        + ring.stream().map(ECSNode::toString).collect(Collectors.joining(", ", "[", "]"))
        + " )";
    //    StringBuilder sb = new StringBuilder();
    //    sb.append("Metadata:\n");
    //    sb.append("::::::::::::::::::::::::::::::::\n");
    //    sb.append("Current number of nodes: ").append(getNodeRing().size()).append("\n");
    //    for (ECSNode node : getNodeRing()) {
    //      sb.append("--------------------------------\n");
    //      sb.append("Node ").append(node.getNodeName()).append("\n");
    //      sb.append("Hash range from ")
    //          .append(node.getLowerRange())
    //          .append(" to ")
    //          .append(node.getNodeHash())
    //          .append("\n");
    //    }
    //    return sb.toString();
  }
}
