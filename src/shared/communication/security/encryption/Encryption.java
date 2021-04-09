package shared.communication.security.encryption;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.security.*;
import java.util.Arrays;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.SecretKeySpec;

public class Encryption {

  public static SecretKeySpec createSecretKeySpec(String stringKey) throws EncryptionException {
    byte[] byteKey;
    try {
      byteKey = stringKey.getBytes("UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new EncryptionException("Creation of symmetric key error " + e.getLocalizedMessage());
    }
    byteKey = Arrays.copyOf(byteKey, 32);
    return new SecretKeySpec(byteKey, "AES");
  }

  public static byte[] encryptBytes(byte[] input, Key key, EncryptionType type)
      throws EncryptionException {
    try {
      Cipher cipher = Cipher.getInstance(type.toString());
      cipher.init(Cipher.ENCRYPT_MODE, key);
      return cipher.doFinal(input);
    } catch (NoSuchAlgorithmException
        | NoSuchPaddingException
        | IllegalBlockSizeException
        | BadPaddingException
        | InvalidKeyException e) {
      throw new EncryptionException("String Decryption Error " + e.getLocalizedMessage());
    }
  }

  public static byte[] decryptBytes(byte[] input, Key key, EncryptionType type)
      throws EncryptionException {
    try {
      Cipher cipher = Cipher.getInstance(type.toString());
      cipher.init(Cipher.DECRYPT_MODE, key);
      return cipher.doFinal(input);
    } catch (NoSuchAlgorithmException
        | NoSuchPaddingException
        | IllegalBlockSizeException
        | BadPaddingException
        | InvalidKeyException e) {
      throw new EncryptionException("String Decryption Error " + e.getLocalizedMessage());
    }
  }

  public static String encryptString(String input, Key key, EncryptionType type)
      throws EncryptionException {
    try {
      Cipher cipher = Cipher.getInstance(type.toString());
      cipher.init(Cipher.ENCRYPT_MODE, key);
      byte[] bytesToEncrypt = input.getBytes(StandardCharsets.UTF_8);
      return java.util.Base64.getEncoder().encodeToString(cipher.doFinal(bytesToEncrypt));
    } catch (NoSuchAlgorithmException
        | NoSuchPaddingException
        | IllegalBlockSizeException
        | BadPaddingException
        | InvalidKeyException e) {
      throw new EncryptionException("String Decryption Error " + e);
    }
  }

  public static String decryptString(String input, Key key, EncryptionType type)
      throws EncryptionException {
    try {
      Cipher cipher = Cipher.getInstance(type.toString());
      cipher.init(Cipher.DECRYPT_MODE, key);
      byte[] bytesToDecrypt = java.util.Base64.getDecoder().decode(input);
      return new String(cipher.doFinal(bytesToDecrypt));
    } catch (NoSuchAlgorithmException
        | NoSuchPaddingException
        | IllegalBlockSizeException
        | BadPaddingException
        | InvalidKeyException e) {
      throw new EncryptionException("String Decryption Error " + e);
    }
  }

  public enum EncryptionType {
    // for symmetric encryption
    AES,
    // for asymmetric encryption
    RSA,
  }
}
