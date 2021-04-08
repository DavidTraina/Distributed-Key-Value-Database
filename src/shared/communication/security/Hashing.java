package shared.communication.security;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import javax.xml.bind.DatatypeConverter;
import org.apache.log4j.Logger;

public class Hashing {

  private static final Logger logger = Logger.getLogger(Hashing.class);

  public static String calculateMD5Hash(String string) {
    MessageDigest md;
    try {
      md = MessageDigest.getInstance("MD5");
    } catch (NoSuchAlgorithmException e) {
      String msg = "Unable to retrieve MD5 Algorithm";
      logger.error(msg, e);
      throw new RuntimeException(msg);
    }
    assert (md != null);
    md.update(string.getBytes());
    byte[] digest = md.digest();
    return DatatypeConverter.printHexBinary(digest).toUpperCase();
  }
}
