package ecs;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import javax.xml.bind.DatatypeConverter;

public class ECSUtils {
  public static boolean checkIfKeyBelongsInRange(String key, String[] ends) {
    MessageDigest md = null;
    try {
      md = MessageDigest.getInstance("MD5");
    } catch (NoSuchAlgorithmException e) {
      e.printStackTrace();
    }
    assert (md != null);
    md.update(key.getBytes());
    byte[] digest = md.digest();
    String hash = DatatypeConverter.printHexBinary(digest).toUpperCase();

    // the node is not wrapping
    if (ends[0].compareTo(ends[1]) < 0) {
      return ends[0].compareTo(hash) <= 0 && ends[1].compareTo(hash) > 0;
    } else {
      return ends[1].compareTo(hash) <= 0 || ends[0].compareTo(hash) > 0;
    }
  }
}
