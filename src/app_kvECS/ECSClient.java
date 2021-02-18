package app_kvECS;

import ecs.ECSNode;
import java.util.Collection;
import java.util.Map;

public class ECSClient implements IECSClient {

  @Override
  public boolean start() {
    // TODO
    return false;
  }

  @Override
  public boolean stop() {
    // TODO
    return false;
  }

  @Override
  public boolean shutdown() {
    // TODO
    return false;
  }

  @Override
  public ECSNode addNode(String cacheStrategy, int cacheSize) {
    // TODO
    return null;
  }

  @Override
  public Collection<ECSNode> addNodes(int count, String cacheStrategy, int cacheSize) {
    // TODO
    return null;
  }

  @Override
  public Collection<ECSNode> setupNodes(int count, String cacheStrategy, int cacheSize) {
    // TODO
    return null;
  }

  @Override
  public boolean awaitNodes(int count, int timeout) throws Exception {
    // TODO
    return false;
  }

  @Override
  public boolean removeNodes(Collection<String> nodeNames) {
    // TODO
    return false;
  }

  @Override
  public Map<String, ECSNode> getNodes() {
    // TODO
    return null;
  }

  @Override
  public ECSNode getNodeByKey(String Key) {
    // TODO
    return null;
  }

  public static void main(String[] args) {
    // TODO
  }
}
