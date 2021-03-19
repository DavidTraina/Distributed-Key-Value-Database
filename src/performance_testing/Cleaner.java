package performance_testing;

import java.io.File;
import java.io.IOException;

public class Cleaner {
  public static void main(String[] args) throws IOException {
    clean();
  }

  public static void clean() throws IOException {
    final File folder = new File(System.getProperty("user.dir"));
    for (final File file : folder.listFiles()) {
      if (file.getAbsolutePath().contains("KeyValueData")) {
        file.delete();
        file.createNewFile();
      }
    }
  }
}
