package testing;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import ecs.ECSUtils;
import org.junit.Test;

public class ECSUtilsTest {
  @Test
  public void testCheckIfKeyBelongsInRangeTrueNormal() {
    String key = "strawberry"; // MD5 hash 495BF9840649EE1EC953D99F8E769889
    assertTrue(
        ECSUtils.checkIfKeyBelongsInRange(
            key,
            new String[] {"495BF9840649EE1EC953D99F8E769888", "FF5BF9840649EE1EC953D99F8E769889"}));
  }

  @Test
  public void testCheckIfKeyBelongsInRangeTrueFlipped() {
    String key = "strawberry"; // MD5 hash 495BF9840649EE1EC953D99F8E769889
    assertTrue(
        ECSUtils.checkIfKeyBelongsInRange(
            key,
            new String[] {"FF5BF9840649EE1EC953D99F8E769889", "495BF9840649EE1EC953D99F8E769890"}));
  }

  @Test
  public void testCheckIfKeyBelongsInRangeFalseFlipped() {
    String key = "AAD"; // MD5 hash 627D0E3109EF246FBFABC0CCDEA0FE43
    assertFalse(
        ECSUtils.checkIfKeyBelongsInRange(
            key,
            new String[] {"F79C4C33DEBFB98AF078BA5F5B1CE310", "16CD404A01E1FCC12AEC2E72719D1B70"}));
  }
}
