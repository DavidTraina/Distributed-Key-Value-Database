package app_kvECS;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class ZKManager {
  private static final Logger logger = Logger.getLogger(ZKManager.class);
  private static ZooKeeper zk;
  private static ZKConnection zkConnection;

  public ZKManager(String zkAddress, int zkPort) {
    initialize(zkAddress, zkPort);
  }

  private void initialize(String zkAddress, int zkPort) {
    zkConnection = new ZKConnection();
    try {
      zk = zkConnection.connect(zkAddress, zkPort);
    } catch (IOException | InterruptedException e) {
      String msg = "Error initializing ZooKeeper connection: ";
      logger.error(msg, e);
      throw new RuntimeException(msg, e);
    }
  }

  public void closeConnection() {
    try {
      zkConnection.close();
    } catch (InterruptedException e) {
      String msg = "Error closing Zookeeper Connection: ";
      logger.error(msg, e);
      throw new RuntimeException(msg, e);
    }
  }

  public void create(String path, byte[] data) {

    try {
      zk.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    } catch (KeeperException.NodeExistsException e) {
      logger.debug("Tried to create node that already exists at " + path);
    } catch (KeeperException | InterruptedException e) {
      String msg = "Error creating ZNode at " + path + " : ";
      logger.error(msg, e);
      throw new RuntimeException(msg, e);
    }
  }

  public void createEphemeral(String path, byte[] data) {

    try {
      zk.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    } catch (KeeperException.NodeExistsException e) {
      logger.debug("Tried to create node that already exists at " + path);
    } catch (KeeperException | InterruptedException e) {
      String msg = "Error creating Ephemeral ZNode at " + path + " : ";
      logger.error(msg, e);
      throw new RuntimeException(msg, e);
    }
  }

  public byte[] getZNodeData(String path, Watcher watcher) {
    try {
      return zk.getData(path, watcher, null);
    } catch (KeeperException | InterruptedException e) {
      String msg = "Error getting data from ZNode at " + path + " : ";
      logger.error(msg, e);
      throw new RuntimeException(msg, e);
    }
  }

  public List<String> getChildrenList(String path, boolean watchFlag) {
    try {
      return zk.getChildren(path, watchFlag);
    } catch (KeeperException.NoNodeException e) {
      return new ArrayList<String>() {};
    } catch (KeeperException | InterruptedException e) {
      String msg = "Error getting children for ZNode at " + path + " : ";
      logger.error(msg, e);
      throw new RuntimeException(msg, e);
    }
  }

  public List<String> getChildrenList(String path, Watcher watcher) {
    try {
      return zk.getChildren(path, watcher);
    } catch (KeeperException.NoNodeException e) {
      return new ArrayList<String>() {};
    } catch (KeeperException | InterruptedException e) {
      String msg = "Error getting children for ZNode at " + path + " : ";
      logger.error(msg, e);
      throw new RuntimeException(msg, e);
    }
  }

  public void update(String path, byte[] data) {
    try {
      zk.setData(path, data, -1);
    } catch (KeeperException | InterruptedException e) {
      String msg = "Error updating version data for ZNode at " + path + " : ";
      logger.error(msg, e);
      throw new RuntimeException(msg, e);
    }
  }

  public void delete(String path) {
    try {
      Stat nodeStat = zk.exists(path, true);
      if (nodeStat == null) {
        return;
      }
      int version = nodeStat.getVersion();
      zk.delete(path, version);
    } catch (KeeperException | InterruptedException e) {
      String msg = "Error deleting ZNode at " + path + " : ";
      logger.error(msg, e);
      throw new RuntimeException(msg, e);
    }
  }
}
