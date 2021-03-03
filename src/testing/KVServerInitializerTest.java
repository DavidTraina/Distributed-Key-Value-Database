package testing;

import app_kvServer.KVServerInitializer;
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
              } catch (IllegalArgumentException e) {
              }
            });
  }
}
