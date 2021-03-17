package ecs;

import static ecs.ECSUtils.checkIfKeyBelongsInRange;

import java.util.ArrayList;
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

  public void clear() {
    this.ring.clear();
  }

  public void update(ECSMetadata newMetadata) {
    singletonECSMetadata.ring = newMetadata.getMetadata();
  }

  public void updateArray(ArrayList<ECSNode> serverData) {
    singletonECSMetadata.ring = serverData;
  }

  public ArrayList<ECSNode> getMetadata() {
    return ring;
  }

  public ECSNode getNodeBasedOnKey(String key) {
    for (ECSNode node : ring) {
      String[] ends = node.getNodeHashRange();
      if (checkIfKeyBelongsInRange(key, ends)) {
        return node;
      }
    }
    System.out.println(
        "This should not be happening. This means that we could not find right node");
    return null;
  }

  public ECSNode getNodeBasedOnName(String name) {
    for (ECSNode node : ring) {
      if (node.getNodeName().equals(name)) {
        return node;
      }
    }
    return null;
  }

  public ECSNode[] getReplicasBasedOnName(String name) {
    int replicas;
    if (ring.size() > 2) {
      replicas = 2;
    } else if (ring.size() == 2) {
      replicas = 1;
    } else {
      return new ECSNode[] {};
    }
    ECSNode node = null;
    ECSNode replica1 = null;
    ECSNode replica2 = null;
    for (int i = 0; i < ring.size(); i++) {
      node = ring.get(i);
      if (i == 0) {
        replica1 = node;
      } else if (i == 1) {
        replica2 = node;
      }
      if (node.getNodeName().equals(name)) {
        if (i < ring.size() - 2) {
          return new ECSNode[] {ring.get(i + 1), ring.get(i + 2)};
        } else if (i < ring.size() - 1) {
          return new ECSNode[] {ring.get(i + 1), replica1};
        } else {
          return new ECSNode[] {replica1, replica2};
        }
      }
    }
    return null;
  }

  public ECSNode[] placeNewNodeOnTheRing(ECSNode newNode) {
    if (ring.size() == 0) {
      newNode.setLowerRange(newNode.getNodeHash());
      ring.add(newNode);
      return new ECSNode[] {newNode};
    }
    int nodeIndex;
    for (nodeIndex = 0; nodeIndex < ring.size(); nodeIndex++) {
      // found my spot
      if (newNode.getNodeHash().compareTo(ring.get(nodeIndex).getNodeHash()) < 0) {
        ECSNode successor = ring.get(nodeIndex);
        newNode.setLowerRange(successor.getLowerRange());
        successor.setLowerRange(newNode.getNodeHash());
        ring.add(nodeIndex, newNode);
        return new ECSNode[] {newNode, successor};
      }
    }
    // hash of this guy is the largest
    ECSNode successor = ring.get(0);
    newNode.setLowerRange(successor.getLowerRange());
    successor.setLowerRange(newNode.getNodeHash());
    ring.add(nodeIndex, newNode);
    return new ECSNode[] {newNode, successor};
  }

  public ECSNode[] removeNodeFromTheRing(String nodeName) {
    int nodeIndex;
    for (nodeIndex = ring.size() - 1; nodeIndex >= 0; nodeIndex--) {
      // found my spot
      if (ring.get(nodeIndex).getNodeName().equals(nodeName)) {
        ECSNode nodeToRemove = ring.get(nodeIndex);
        ECSNode predecessor;
        if (nodeIndex == ring.size() - 1) {
          predecessor = ring.get(0);
        } else {
          predecessor = ring.get(nodeIndex + 1);
        }
        predecessor.setLowerRange(nodeToRemove.getLowerRange());
        ring.remove(nodeIndex);
        return new ECSNode[] {nodeToRemove, predecessor};
      }
    }
    return null;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    if (singletonECSMetadata == null) {
      sb.append("\nMetadata is not initialized!");
    } else {
      sb.append("Metadata:\n");
      sb.append("::::::::::::::::::::::::::::::::\n");
      sb.append("Current number of nodes: ")
          .append(singletonECSMetadata.getMetadata().size())
          .append("\n");
      for (ECSNode node : singletonECSMetadata.getMetadata()) {
        sb.append("--------------------------------\n");
        sb.append("Node ").append(node.getNodeName()).append("\n");
        sb.append("Hash range from ")
            .append(node.getLowerRange())
            .append(" to ")
            .append(node.getNodeHash())
            .append("\n");
      }
    }
    return sb.toString();
  }
}
