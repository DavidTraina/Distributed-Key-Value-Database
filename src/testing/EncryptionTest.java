package testing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.nio.charset.StandardCharsets;
import java.security.PrivateKey;
import java.security.PublicKey;
import javax.crypto.spec.SecretKeySpec;
import org.junit.BeforeClass;
import org.junit.Test;
import shared.communication.security.KeyGenerator;
import shared.communication.security.encryption.Encryption;
import shared.communication.security.encryption.EncryptionException;

public class EncryptionTest {
  private static PublicKey publicKey;
  private static PrivateKey privateKey;
  private static SecretKeySpec symmetricKey;

  @BeforeClass
  public static void startServer() {
    KeyGenerator generator = new KeyGenerator(1024);
    generator.createKeys();
    publicKey = generator.getPublicKey();
    privateKey = generator.getPrivateKey();
    try {
      symmetricKey = Encryption.createSecretKeySpec("abc");
    } catch (EncryptionException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testGeneratedSymmetricKeyIsValid() {
    assert (symmetricKey != null);
  }

  @Test
  public void testEncryptionAndDecryptionWorksOnBytesRSA() {
    String msg = "Hello World!";
    try {
      byte[] encryptedMsg;
      encryptedMsg =
          Encryption.encryptBytes(
              msg.getBytes(StandardCharsets.UTF_8), privateKey, Encryption.EncryptionType.RSA);
      byte[] decryptedMsg =
          Encryption.decryptBytes(encryptedMsg, publicKey, Encryption.EncryptionType.RSA);
      String msg2 = new String(decryptedMsg);

      assertEquals(msg, msg2);

    } catch (EncryptionException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testEncryptionAndDecryptionWorksOnStringsRSA() {
    String msg = "Hello World!";
    try {
      String encryptedMsg =
          Encryption.encryptString(msg, privateKey, Encryption.EncryptionType.RSA);
      String msg2 =
          Encryption.decryptString(encryptedMsg, publicKey, Encryption.EncryptionType.RSA);
      assertEquals(msg, msg2);

    } catch (EncryptionException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testEncryptionAndDecryptionWorksOnBytesAES() {
    String msg = "Hello World!";
    try {
      byte[] encryptedMsg;
      encryptedMsg =
          Encryption.encryptBytes(
              msg.getBytes(StandardCharsets.UTF_8), symmetricKey, Encryption.EncryptionType.AES);
      byte[] decryptedMsg =
          Encryption.decryptBytes(encryptedMsg, symmetricKey, Encryption.EncryptionType.AES);
      String msg2 = new String(decryptedMsg);

      assertEquals(msg, msg2);

    } catch (EncryptionException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testEncryptionAndDecryptionWorksOnStringsAES() {
    String msg = "Hello World!";
    try {
      String encryptedMsg =
          Encryption.encryptString(msg, symmetricKey, Encryption.EncryptionType.AES);
      String msg2 =
          Encryption.decryptString(encryptedMsg, symmetricKey, Encryption.EncryptionType.AES);
      assertEquals(msg, msg2);

    } catch (EncryptionException e) {
      e.printStackTrace();
      fail();
    }
  }
}
