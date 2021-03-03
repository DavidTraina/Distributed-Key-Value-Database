package testing;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.junit.BeforeClass;
import org.junit.Test;

public class InitializeTests {
  @BeforeClass
  public static void initBeforeAllTests() {
    try {
      new LogSetup("logs/testing/test.log", Level.ERROR, false);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testPass() {
    assertTrue(true);
  }
}
