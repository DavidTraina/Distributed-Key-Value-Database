package shared.communication.security;

import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;

public class KeyLoader {
  public static PrivateKey getPrivateKey(String base64EncodedKey) throws InvalidKeySpecException {

    byte[] keyBytes = Base64.getDecoder().decode(base64EncodedKey);
    PKCS8EncodedKeySpec spec = new PKCS8EncodedKeySpec(keyBytes);
    KeyFactory kf = null;
    try {
      kf = KeyFactory.getInstance("RSA");
    } catch (NoSuchAlgorithmException e) {
      e.printStackTrace(); // Should not happen
      return null;
    }
    return kf.generatePrivate(spec);
  }

  public static PublicKey getPublicKey(String base64EncodedKey) throws InvalidKeySpecException {

    byte[] keyBytes = Base64.getDecoder().decode(base64EncodedKey);

    X509EncodedKeySpec spec = new X509EncodedKeySpec(keyBytes);
    KeyFactory kf = null;
    try {
      kf = KeyFactory.getInstance("RSA");
    } catch (NoSuchAlgorithmException e) {
      e.printStackTrace(); // Should not happen
      return null;
    }
    return kf.generatePublic(spec);
  }

  //    public static void main(String[] args) throws IOException {
  //      byte[] fileData = Files.readAllBytes(Paths.get("private_key.der"));
  //      System.out.print(Base64.getEncoder().encodeToString(fileData));
  //    }
}
