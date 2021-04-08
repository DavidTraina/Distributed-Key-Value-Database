package app_kvServer.data.storage;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import org.apache.log4j.Logger;
import shared.communication.security.encryption.AESEncryption;
import shared.communication.security.encryption.AESEncryptionException;

public class StorageUnit {
  private static final Logger logger = Logger.getLogger(StorageUnit.class);
  public final StorageType storageType;
  public final String key;
  public String value;
  public String kvCheck;

  public StorageUnit(final String key, final String value, final String kvCheck) {
    storageType = StorageType.KV;
    this.key = key;
    this.value = value;
    this.kvCheck = kvCheck;
  }

  public static StorageUnit deserialize(String line, AESEncryption encryption)
      throws StorageUnitException {
    Gson gson = new Gson();
    String storageJson = line;
    try {
      if (encryption != null) {
        storageJson = encryption.decrypt(storageJson);
      }
      StorageUnit deserializedUnit = gson.fromJson(storageJson, StorageUnit.class);
      return deserializedUnit;
    } catch (JsonSyntaxException | AESEncryptionException e) {
      logger.error("Problem with decrypting JSON \n" + storageJson);
      throw new StorageUnitException("deserialization failed " + e.getLocalizedMessage());
    }
  }

  /** @return a string ready for transporting over the network. */
  public String serialize(AESEncryption encryption) throws StorageUnitException {
    Gson gson = new Gson();
    String storageJson = gson.toJson(this);
    if (encryption != null) {
      try {
        return encryption.encrypt(storageJson);
      } catch (Exception e) {
        logger.error("Encryption failed! \n" + storageJson);
        throw new StorageUnitException("serialization failed " + e.getLocalizedMessage());
      }
    }
    return storageJson;
  }

  public enum StorageType {
    KV
  }
}
