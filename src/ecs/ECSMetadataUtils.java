package ecs;

import static ecs.ECSUtils.checkIfKeyBelongsInRange;

import java.util.ArrayList;

public class ECSMetadataUtils {
  public static ECSNode findPredecessor(String nodeName, ArrayList<ECSNode> ring) {
    if (ring.size() == 1) return null;

    for (int i = 0; i < ring.size(); i++) {
      if (ring.get(i).getNodeName().equals(nodeName)) {
        if (i > 0) return ring.get(i - 1);
        return ring.get(ring.size() - 1);
      }
    }
    return null;
  }

  public static ECSNode findSuccessor(String nodeName, ArrayList<ECSNode> ring) {
    if (ring.size() == 1) return null;

    for (int i = 0; i < ring.size(); i++) {
      if (ring.get(i).getNodeName().equals(nodeName)) {
        if (i < ring.size() - 1) return ring.get(i + 1);
        return ring.get(0);
      }
    }
    return null;
  }

  public static ECSNode getNodeBasedOnKey(String key, ArrayList<ECSNode> ring) {
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

  public static ECSNode getNodeBasedOnName(String name, ArrayList<ECSNode> ring) {
    for (ECSNode node : ring) {
      if (node.getNodeName().equals(name)) {
        return node;
      }
    }
    return null;
  }

  public static ECSNode[] getReplicasBasedOnName(String name, ArrayList<ECSNode> ring) {
    int replicas;
    if (ring.size() > 2) {
      replicas = 2;
    } else if (ring.size() == 2) {
      replicas = 1;
    } else {
      return new ECSNode[] {};
    }
    ECSNode node;
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
        if (replicas == 2) {
          if (i < ring.size() - 2) {
            return new ECSNode[] {ring.get(i + 1), ring.get(i + 2)};
          } else if (i < ring.size() - 1) {
            return new ECSNode[] {ring.get(i + 1), replica1};
          } else {
            return new ECSNode[] {replica1, replica2};
          }
        } else {
          if (i < ring.size() - 1) {
            return new ECSNode[] {ring.get(i + 1)};
          } else {
            return new ECSNode[] {replica1};
          }
        }
      }
    }
    return null;
  }

  public static ECSNode[] getNodesWhereIAmReplicaBasedOnName(String name, ArrayList<ECSNode> ring) {
    int replicas;
    if (ring.size() > 2) {
      replicas = 2;
    } else if (ring.size() == 2) {
      replicas = 1;
    } else {
      return new ECSNode[] {};
    }
    ECSNode node;
    ECSNode replica1 = null;
    ECSNode replica2 = null;
    for (int i = ring.size() - 1; i >= 0; i--) {
      node = ring.get(i);
      if (i == ring.size() - 1) {
        replica1 = node;
      } else if (i == ring.size() - 2) {
        replica2 = node;
      }
      if (node.getNodeName().equals(name)) {
        if (replicas == 2) {
          if (i >= 2) {
            return new ECSNode[] {ring.get(i - 1), ring.get(i - 2)};
          } else if (i >= 1) {
            return new ECSNode[] {ring.get(0), replica1};
          } else {
            return new ECSNode[] {replica1, replica2};
          }
        } else {
          if (i >= 1) {
            return new ECSNode[] {ring.get(i - 1)};
          } else {
            return new ECSNode[] {replica1};
          }
        }
      }
    }
    return null;
  }

  public static ECSNode[] placeNewNodeOnTheRing(ECSNode newNode, ArrayList<ECSNode> ring) {
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

  public static ECSNode[] removeNodeFromTheRing(String nodeName, ArrayList<ECSNode> ring) {
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
}
