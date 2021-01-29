package app_kvServer.data.cache;

import java.util.NoSuchElementException;

public class NoOpCache<K, V> extends ThreadSafeCache<K, V> {

  protected NoOpCache(int maxSize, CacheStrategy strategy) {
    super(maxSize, strategy);
  }

  @Override
  public V get(K key) throws NoSuchElementException {
    throw new NoSuchElementException("No Op Cache");
  }

  @Override
  public void put(K key, V value) {}

  @Override
  public void remove(K key) {}

  @Override
  public boolean containsKey(K key) {
    return false;
  }

  @Override
  public void purge() {}
}
