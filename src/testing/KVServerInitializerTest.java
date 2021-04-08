package testing;

import static org.junit.Assert.fail;

import app_kvServer.KVServerInitializer;
import app_kvServer.data.SynchronizedKVManager;
import client.ByzantineException;
import java.lang.reflect.Field;
import java.util.stream.Stream;
import org.junit.Test;

public class KVServerInitializerTest {

  @Test
  public void testRobustToInput() {
    Stream.of(
            " this aint three arguments",
            "this neither",
            "port cachesize cachestrat",
            " 1 2 3",
            "1 -1 FIFO",
            "",
            "50000  1000   notastrat ")
        .map(cmd -> cmd.split("\\s+"))
        .forEach(
            cmd -> {
              try {
                KVServerInitializer.main(cmd);
              } catch (IllegalArgumentException | ByzantineException e) {
              }
            });
  }

  // zookeper way of initialization doesn't on github work because there is no zookeper there
  @Test
  public void testAcceptsValidInput() {
    Stream.of(
            //            "5041 0 LRU localhost 2181 true 127.0.0.1:5041",
            //            "5041 0 LRU localhost 2181 127.0.0.1:5041",
            "5041 0 LRU false", "5041 0 LRU")
        .map(cmd -> cmd.split("\\s+"))
        .forEach(
            cmd -> {
              Field instance;
              try {
                instance = SynchronizedKVManager.class.getDeclaredField("INSTANCE");
                instance.setAccessible(true);
                instance.set(null, null);
              } catch (NoSuchFieldException | IllegalAccessException e) {
                fail();
              }
              try {
                KVServerInitializer.main(cmd);
              } catch (ByzantineException e) {
                fail();
              }
            });
  }
}
