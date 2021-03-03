package performance_testing;

import client.KVStore;
import client.KVStoreException;
import info.debatty.java.datasets.enron.Email;
import java.net.InetAddress;
import java.util.Random;
import java.util.Spliterator;
import java.util.concurrent.Callable;
import shared.communication.messages.KVMessage;

public class EnronClientWorker implements Callable<Metrics> {

  private final KVStore store;
  private final float writeRatio;
  private final Spliterator<Email> emails;
  Metrics metrics = new Metrics();

  public EnronClientWorker(
      final InetAddress address,
      final int port,
      final float writeRatio,
      final Spliterator<Email> emails) {
    this.store = new KVStore(address, port);
    this.writeRatio = writeRatio;
    this.emails = emails;
  }

  @Override
  public Metrics call() {
    try {
      store.connect();
    } catch (KVStoreException e) {
      e.printStackTrace();
    }
    emails.forEachRemaining(
        email -> {
          assert email != null;
          System.out.println(email.getMessageID());
          try {
            String key = EnronClientWorker.sanitize(email.getMessageID());
            String value =
                "subj:  " + sanitize(email.getSubject()) + ", from:  " + sanitize(email.getFrom());
            Random random = new Random();
            boolean isWrite = random.nextFloat() <= writeRatio;
            if (isWrite) {
              doPut(key, value);
            } else {
              doGet(key);
            }
          } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
          }
        });
    try {
      store.disconnect();
    } catch (KVStoreException e) {
      e.printStackTrace();
    }
    return this.metrics;
  }

  private static String sanitize(String s) {
    return s.trim().replace('\n', '|').replace(' ', '_');
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
