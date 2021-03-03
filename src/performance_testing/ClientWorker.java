package performance_testing;

import client.KVStore;
import client.KVStoreException;
import java.net.InetAddress;
import java.security.SecureRandom;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import shared.communication.messages.KVMessage;

public class ClientWorker implements Callable<Metrics> {

  private final KVStore store;
  private final float writeRatio;
  private final int numRequests;
  Metrics metrics = new Metrics();

  public ClientWorker(
      final InetAddress address, final int port, final float writeRatio, final int numRequests)
      throws KVStoreException {
    this.store = new KVStore(address, port);
    this.writeRatio = writeRatio;
    this.numRequests = numRequests;
  }

  // Copied from stackoverflow:
  // https://stackoverflow.com/questions/39222044/generate-random-string-in-java
  public static String createRandomCode(int codeLength, String id) {
    return new SecureRandom()
        .ints(codeLength, 0, id.length())
        .mapToObj(id::charAt)
        .map(Object::toString)
        .collect(Collectors.joining());
  }

  @Override
  public Metrics call() throws Exception {
    Random random = new Random();
    store.connect();
    String value, key;
    int numRequestsCompleted = 0;

    while (numRequestsCompleted != numRequests) {
      // Choose a small key to have collisions assuming high numRequests
      key = createRandomCode(3, "ABCDEF");
      // generate a random integer between 0 and 1000
      int valueLen = random.nextInt(1000);
      value = createRandomCode(valueLen, "ABCDEFGHIJKLMNOPQRSTUVWXYZ");

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
      numRequestsCompleted += 1;
    }

    store.disconnect();
    return this.metrics;
  }

  private void doPut(String key, String value) {
    try {
      long start = System.nanoTime();
      KVMessage.StatusType status = store.put(key, value).getStatus();
      assert (status == KVMessage.StatusType.PUT_SUCCESS
          || status == KVMessage.StatusType.PUT_UPDATE);
      long end = System.nanoTime();
      metrics.updatePutLatency(end - start);
    } catch (KVStoreException e) {
      e.printStackTrace();
    }
  }

  private void doGet(String key) {
    try {
      long start = System.nanoTime();
      KVMessage.StatusType status = store.get(key).getStatus();
      assert (status == KVMessage.StatusType.GET_SUCCESS
          || status == KVMessage.StatusType.GET_ERROR);
      long end = System.nanoTime();
      metrics.updateGetLatency(end - start);
    } catch (KVStoreException e) {
      e.printStackTrace();
    }
  }
}
