package app_kvServer.cache;

import app_kvServer.KVStore;

public abstract class Cache implements KVStore {
  protected int maxSize;

  public int getMaxSize() {
    return maxSize;
  }

  public void setMaxSize(int maxSize) {
    this.maxSize = maxSize;
  }
}
