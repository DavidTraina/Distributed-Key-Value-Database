package shared.communication.security.encryption;

import java.nio.charset.StandardCharsets;
import java.security.*;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;

public class AsymmetricEncryption {
  public static byte[] encryptBytes(byte[] input, PrivateKey key)
      throws AsymmetricEncryptionException {
    try {
      Cipher cipher = Cipher.getInstance("RSA");
      cipher.init(Cipher.ENCRYPT_MODE, key);
      return cipher.doFinal(input);
    } catch (NoSuchAlgorithmException
        | NoSuchPaddingException
        | IllegalBlockSizeException
        | BadPaddingException
        | InvalidKeyException e) {
      throw new AsymmetricEncryptionException("String Decryption Error " + e);
    }
  }

  public static byte[] decryptBytes(byte[] input, PublicKey key)
      throws AsymmetricEncryptionException {
    try {
      Cipher cipher = Cipher.getInstance("RSA");
      cipher.init(Cipher.DECRYPT_MODE, key);
      return cipher.doFinal(input);
    } catch (NoSuchAlgorithmException
        | NoSuchPaddingException
        | IllegalBlockSizeException
        | BadPaddingException
        | InvalidKeyException e) {
      throw new AsymmetricEncryptionException("String Decryption Error " + e);
    }
  }

  public static String encryptString(String input, PrivateKey key)
      throws AsymmetricEncryptionException {
    try {
      Cipher cipher = Cipher.getInstance("RSA");
      cipher.init(Cipher.ENCRYPT_MODE, key);
      byte[] bytesToEncrypt = input.getBytes(StandardCharsets.UTF_8);
      return java.util.Base64.getEncoder().encodeToString(cipher.doFinal(bytesToEncrypt));
    } catch (NoSuchAlgorithmException
        | NoSuchPaddingException
        | IllegalBlockSizeException
        | BadPaddingException
        | InvalidKeyException e) {
      throw new AsymmetricEncryptionException("String Decryption Error " + e);
    }
  }

  public static String decryptString(String input, PublicKey key)
      throws AsymmetricEncryptionException {
    try {
      Cipher cipher = Cipher.getInstance("RSA");
      cipher.init(Cipher.DECRYPT_MODE, key);
      byte[] bytesToDecrypt = java.util.Base64.getDecoder().decode(input);
      return new String(cipher.doFinal(bytesToDecrypt));
    } catch (NoSuchAlgorithmException
        | NoSuchPaddingException
        | IllegalBlockSizeException
        | BadPaddingException
        | InvalidKeyException e) {
      throw new AsymmetricEncryptionException("String Decryption Error " + e);
    }
  }
}
