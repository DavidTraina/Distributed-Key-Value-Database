package testing;

import app_kvClient.KVClient;
import java.util.stream.Stream;
import org.junit.Test;

public class KVClientTest {

  @Test
  public void testRobustToInput() {
    KVClient kvClient = new KVClient();
    Stream.of(
            "  a bunch of  Ran dom   stuff  ",
            "quit",
            "quit  k k u ",
            "disconnect",
            "logLevel notalevel",
            "help me",
            "",
            "put k   v   v  ",
            "put k",
            "get",
            "get  key  ",
            "connect notahost notaport",
            "connect 123 456",
            "connect 1 2 3 4 5",
            "quit 1 2 3 4 5",
            "get 1 2 3 4 5",
            "put 1 2 3 4 5",
            "help 1 2 3 4 5",
            "logLevel 1 2 3 4 5",
            "disconnect 1 2 3 4 5")
        .forEach(kvClient::handleCommand);
  }
}
