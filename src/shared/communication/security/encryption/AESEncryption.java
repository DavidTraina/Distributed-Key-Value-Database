package shared.communication.security.encryption;

/**
 * * Used
 * https://howtodoinjava.com/java/java-security/java-aes-encryption-example/?fbclid=IwAR3qh7jvI0vtqYUM2gUle2rBb4MIp56d8yDdZ-d9FRnCYpWet4svJZx2AeE
 * as a reference
 */
import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.util.Arrays;
import java.util.Base64;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.spec.SecretKeySpec;
import org.apache.log4j.Logger;

public class AESEncryption {
  private static final Logger logger = Logger.getLogger(AESEncryption.class);
  private final SecretKeySpec key;
  private final Cipher cipher;

  public AESEncryption(String stringKey) throws AESEncryptionException {
    try {
      byte[] byteKey = stringKey.getBytes("UTF-8");
      byteKey = Arrays.copyOf(byteKey, 32);
      this.key = new SecretKeySpec(byteKey, "AES");
      this.cipher = Cipher.getInstance("AES");
    } catch (Exception e) {
      logger.error("Failed while creating AESEncryption object");
      throw new AESEncryptionException("could not create AESEncryption " + e.getLocalizedMessage());
    }
  }

  public String encrypt(String strToEncrypt) throws AESEncryptionException {
    try {
      cipher.init(Cipher.ENCRYPT_MODE, key);
      byte[] bytesToEncrypt = strToEncrypt.getBytes("UTF-8");
      return Base64.getEncoder().encodeToString(cipher.doFinal(bytesToEncrypt));
    } catch (InvalidKeyException
        | UnsupportedEncodingException
        | IllegalBlockSizeException
        | BadPaddingException e) {
      logger.error("Failed while encrypting a storage unit");
      throw new AESEncryptionException("error on encryption " + e.getLocalizedMessage());
    }
  }

  public String decrypt(String strToDecrypt) throws AESEncryptionException {
    try {
      cipher.init(Cipher.DECRYPT_MODE, key);
      byte[] bytesToDecrypt = Base64.getDecoder().decode(strToDecrypt);
      return new String(cipher.doFinal(bytesToDecrypt));
    } catch (InvalidKeyException | IllegalBlockSizeException | BadPaddingException e) {
      logger.error("Failed while decrypting a storage unit");
      throw new AESEncryptionException("error on decryption " + e.getLocalizedMessage());
    }
  }
}
