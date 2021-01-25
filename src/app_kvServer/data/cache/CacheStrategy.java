package app_kvServer.data.cache;

/** Correspond to distinct implementations of app_kvServer.cache.ThreadSafeCache */
public enum CacheStrategy {
  LRU,
  LFU,
  FIFO,
  CONCURRENT, // <-- Use this one to go 𝘍 𝘈 𝘚 𝘛
}
