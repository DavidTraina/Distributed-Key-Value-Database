package app_kvServer.data.storage;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import java.util.ArrayList;
import java.util.UUID;
import javax.crypto.spec.SecretKeySpec;
import org.apache.log4j.Logger;
import shared.communication.security.encryption.Encryption;
import shared.communication.security.encryption.EncryptionException;

public class StorageUnit {
  private static final Logger logger = Logger.getLogger(StorageUnit.class);
  public final String key;
  public String value;
  public final ArrayList<UUID> subscribers = new ArrayList<>();
  public String uniqueID;
  public String MAC;

  public StorageUnit(
      final String key, final String value, final String uniqueID, final String MAC) {
    this.key = key;
    this.value = value;
    this.uniqueID = uniqueID;
    this.MAC = MAC;
  }

  public static StorageUnit deserialize(String line, SecretKeySpec encryption)
      throws StorageUnitException {
    Gson gson = new Gson();
    String storageJson = line;
    try {
      if (encryption != null) {
        storageJson =
            Encryption.decryptString(storageJson, encryption, Encryption.EncryptionType.AES);
      }
      StorageUnit deserializedUnit = gson.fromJson(storageJson, StorageUnit.class);
      return deserializedUnit;
    } catch (JsonSyntaxException | EncryptionException e) {
      logger.error("Problem with decrypting JSON \n" + storageJson);
      throw new StorageUnitException("deserialization failed " + e.getLocalizedMessage());
    }
  }

  /** @return a string ready for transporting over the network. */
  public String serialize(SecretKeySpec encryption) throws StorageUnitException {
    Gson gson = new Gson();
    String storageJson = gson.toJson(this);
    if (encryption != null) {
      try {
        return Encryption.encryptString(storageJson, encryption, Encryption.EncryptionType.AES);
      } catch (Exception e) {
        logger.error("Encryption failed! \n" + storageJson);
        throw new StorageUnitException("serialization failed " + e.getLocalizedMessage());
      }
    }
    return storageJson;
  }
}
