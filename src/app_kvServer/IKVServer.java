package app_kvServer;

import app_kvServer.data.cache.CacheStrategy;
import java.util.NoSuchElementException;

public interface IKVServer {
  /**
   * Get the port number of the server
   *
   * @return port number
   */
  int getPort();

  /**
   * Get the hostname of the server
   *
   * @return hostname of server
   */
  String getHostname();

  /**
   * Get the cache strategy of the server
   *
   * @return cache strategy
   */
  CacheStrategy getCacheStrategy();

  /**
   * Get the cache size
   *
   * @return cache size
   */
  int getCacheSize();

  /**
   * Check if key is in storage. NOTE: does not modify any other properties
   *
   * @return true if key in storage, false otherwise
   */
  boolean inStorage(String key);

  /**
   * Check if key is in storage. NOTE: does not modify any other properties
   *
   * @return true if key in storage, false otherwise
   */
  boolean inCache(String key);

  /**
   * Get the value associated with the key
   *
   * @return value associated with key
   * @throws NoSuchElementException when key not in the key range of the server
   */
  String getKV(String key) throws NoSuchElementException;

  /**
   * Put the key-value pair into storage
   *
   * @throws Exception when key not in the key range of the server
   */
  void putKV(String key, String value) throws Exception;

  /** Clear the local cache of the server */
  void clearCache();

  /** Clear the storage of the server */
  void clearStorage();

  /** Starts running the server */
  void run();

  /**
   * Abruptly stop the server without any additional actions NOTE: this includes performing saving
   * to storage
   */
  void kill();

  /** Gracefully stop the server, can perform any additional actions */
  void close();
}
