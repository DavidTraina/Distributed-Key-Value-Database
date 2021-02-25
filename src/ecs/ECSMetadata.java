package ecs;

import static ecs.ECSUtils.checkIfKeyBelongsInRange;

import java.util.ArrayList;

public class ECSMetadata {

  private static ECSMetadata singletonECSMetadata = null;

  private ArrayList<ECSNode> serverData;

  private ECSMetadata() {}

  public void clear() {
    this.serverData.clear();
  }

  public static void initialize(ArrayList<ECSNode> serverData) {
    assert (singletonECSMetadata != null);
    singletonECSMetadata.serverData = serverData;
  }

  public static ECSMetadata getInstance() {
    if (singletonECSMetadata == null) {
      singletonECSMetadata = new ECSMetadata();
    }
    return singletonECSMetadata;
  }

  public synchronized void update(ECSMetadata newMetadata) {
    singletonECSMetadata.serverData = newMetadata.getMetadata();
  }

  public ArrayList<ECSNode> getMetadata() {
    return serverData;
  }

  public ECSNode getNodeBasedOnKey(String key) {
    for (ECSNode node : serverData) {
      String[] ends = node.getNodeHashRange();
      if (checkIfKeyBelongsInRange(key, ends)) {
        return node;
      }
    }
    System.out.println(
        "This should not be happening. This means that we could not find right node");
    return null;
  }
}
