package performance_testing;

import client.KVStore;
import client.KVStoreException;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import shared.communication.messages.KVMessage;

public class ClientWorker implements Callable<Metrics> {

  private final KVStore store;
  private final float writeRatio;
  private final int numRequests;
  private final HashMap<String, String> data;
  Metrics metrics = new Metrics();
  final int id;

  public ClientWorker(
      int id,
      final InetAddress address,
      final int port,
      final float writeRatio,
      final int numRequests,
      HashMap<String, String> data)
      throws KVStoreException {
    this.id = id;
    this.store = new KVStore(address, port);
    this.writeRatio = writeRatio;
    this.numRequests = numRequests;
    this.data = data;
  }

  @Override
  public Metrics call() throws Exception {
    Random random = new Random();
    store.connect();
    String value, key;
    int i = 0;
    int size = data.size();
    for (Map.Entry<String, String> entry : data.entrySet()) {
      key = entry.getKey();
      value = entry.getValue();

      if (value.length() == 0) {
        value = null;
      }
      // Make a measurement to determine isWrite
      boolean isWrite = random.nextFloat() <= writeRatio;

      if (isWrite) {
        doPut(key, value);
      } else {
        doGet(key);
      }
      i++;
      if (i % (size / 100) == 0) {
        System.out.println("Client Worker " + id + " completed " + ((double) i / (double) size));
      }
    }

    store.disconnect();
    return this.metrics;
  }

  private void doPut(String key, String value) {
    try {
      long start = System.nanoTime();
      KVMessage.StatusType status = store.put(key, value).getStatus();
      long end = System.nanoTime();
      assert (status == KVMessage.StatusType.PUT_SUCCESS
          || status == KVMessage.StatusType.PUT_UPDATE);
      metrics.updatePutLatency(end - start);
    } catch (KVStoreException e) {
      e.printStackTrace();
    }
  }

  private void doGet(String key) {
    try {
      long start = System.nanoTime();
      KVMessage.StatusType status = store.get(key).getStatus();
      long end = System.nanoTime();
      assert (status == KVMessage.StatusType.GET_SUCCESS
          || status == KVMessage.StatusType.GET_ERROR);
      metrics.updateGetLatency(end - start);
    } catch (KVStoreException e) {
      e.printStackTrace();
    }
  }
}
