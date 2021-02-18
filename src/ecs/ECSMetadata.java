package ecs;

import java.util.ArrayList;

public class ECSMetadata {
  private ArrayList<ECSNode> serverData;

  public ECSMetadata(ArrayList<ECSNode> serverData) {
    this.serverData = serverData;
  }

  public ArrayList<ECSNode> getMetadata() {
    return serverData;
  }
}
