package shared.communication.security;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import org.apache.log4j.Logger;

public class KeyGenerator {

  private static final Logger logger = Logger.getLogger(KeyGenerator.class);
  private KeyPairGenerator keyGen;
  private PrivateKey privateKey;
  private PublicKey publicKey;

  public KeyGenerator(int keyLength) {
    try {
      this.keyGen = KeyPairGenerator.getInstance("RSA");
    } catch (NoSuchAlgorithmException e) {
      logger.error("No such algorithm for keygen");
      e.printStackTrace();
    }
    this.keyGen.initialize(keyLength);
  }

  public void createKeys() {
    KeyPair pair = this.keyGen.generateKeyPair();
    this.privateKey = pair.getPrivate();
    this.publicKey = pair.getPublic();
  }

  public PrivateKey getPrivateKey() {
    return this.privateKey;
  }

  public PublicKey getPublicKey() {
    return this.publicKey;
  }
}
