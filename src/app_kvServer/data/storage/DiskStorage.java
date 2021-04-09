package app_kvServer.data.storage;

import static shared.communication.messages.DataTransferMessage.DataTransferMessageType.DATA_TRANSFER_FAILURE;
import static shared.communication.messages.DataTransferMessage.DataTransferMessageType.DATA_TRANSFER_REQUEST;
import static shared.communication.messages.DataTransferMessage.DataTransferMessageType.DATA_TRANSFER_SUCCESS;
import static shared.communication.messages.KVMessage.StatusType.PUT;
import static shared.communication.messages.KVMessage.StatusType.PUT_ERROR;
import static shared.communication.messages.KVMessage.StatusType.SUBSCRIBE;
import static shared.communication.messages.KVMessage.StatusType.SUBSCRIBE_ERROR;
import static shared.communication.messages.KVMessage.StatusType.SUBSCRIBE_SUCCESS;
import static shared.communication.messages.KVMessage.StatusType.UNSUBSCRIBE;
import static shared.communication.messages.KVMessage.StatusType.UNSUBSCRIBE_ERROR;
import static shared.communication.messages.KVMessage.StatusType.UNSUBSCRIBE_SUCCESS;

import ecs.ECSUtils;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;
import java.util.UUID;
import javax.crypto.spec.SecretKeySpec;
import org.apache.log4j.Logger;
import shared.communication.messages.DataTransferMessage;
import shared.communication.messages.ECSMessage;
import shared.communication.messages.KVMessage;
import shared.communication.security.Verifier;
import shared.communication.security.encryption.Encryption;
import shared.communication.security.encryption.EncryptionException;

public class DiskStorage {
  private static final Logger logger = Logger.getLogger(DiskStorage.class);
  private final File storageFile;
  private final File replica1File;
  private final File replica2File;
  private final String uniqueID;
  private final Object diskWriteLock = new Object();
  private final SecretKeySpec encryption;
  private final HashSet<String> seenIDs = new HashSet<>();

  public DiskStorage(final String uniqueID, boolean encrypted) throws DiskStorageException {
    if (encrypted) {
      Random random = new Random();
      StringBuilder encryptionKey = new StringBuilder();
      for (int i = 0; i < 16; i++) {
        encryptionKey.append(random.nextInt(9));
      }
      try {
        this.encryption = Encryption.createSecretKeySpec(encryptionKey.toString());
      } catch (EncryptionException e) {
        logger.error("Failed on creating encryption key for storage");
        throw new DiskStorageException(e.getLocalizedMessage());
      }
    } else {
      this.encryption = null;
    }
    this.uniqueID = uniqueID;
    this.storageFile = new File("KeyValueData_" + uniqueID + ".txt");
    this.replica1File = new File("KeyValueData_" + uniqueID + "_replica1.txt");
    this.replica2File = new File("KeyValueData_" + uniqueID + "_replica2.txt");
    // remove existing storage to start fresh
    if (storageFile.exists() && !storageFile.delete()) {
      throw new DiskStorageException("Unable to delete file " + storageFile.getAbsolutePath());
    }
    if (replica1File.exists() && !replica1File.delete()) {
      throw new DiskStorageException("Unable to delete file " + replica1File.getAbsolutePath());
    }
    if (replica2File.exists() && !replica2File.delete()) {
      throw new DiskStorageException("Unable to delete file " + replica2File.getAbsolutePath());
    }
    try {
      for (int i = 0; i < 10; i++) {
        storageFile.createNewFile();
        replica1File.createNewFile();
        replica2File.createNewFile();
        if (storageFile.exists() && replica1File.exists() && replica2File.exists()) {
          break;
        }
      }
      logger.info("Storage file " + storageFile.getAbsolutePath() + " created.");
      logger.info("Storage replica 1 " + replica1File.getAbsolutePath() + " created.");
      logger.info("Storage replica 2 " + replica2File.getAbsolutePath() + " created.");
    } catch (Exception e) {
      logger.error("Could not establish connection to disk storage.");
      throw new DiskStorageException(e.getLocalizedMessage());
    }
  }

  public KVMessage get(final KVMessage request, StorageType storageType) {
    assert (request != null);
    if (seenIDs.contains(request.getMAC())) {
      logger.error("The MAC is the repeated MAC. Request will not be served");
      return new KVMessage(
          request.getKey(),
          request.getValue(),
          request.getClientId(),
          KVMessage.StatusType.GET_ERROR,
          request.getRequestId());
    }

    logger.info("GET request for " + request.getKey() + " using storage: " + storageType.name());
    File workingFile = correctFileBasedOnEnum(storageType);
    String requestKey = request.getKey();
    assert (request.getValue() == null);
    String requestValue = null;

    BufferedReader reader;
    try {
      reader = new BufferedReader(new FileReader(workingFile), 16384);
      String entry;

      while ((entry = reader.readLine()) != null) {
        StorageUnit currentUnit = StorageUnit.deserialize(entry.trim(), encryption);
        String key = currentUnit.key;

        if (key.equals(requestKey)) {
          requestValue = currentUnit.value;
          break;
        }
      }
      reader.close();
      if (requestValue != null) {
        logger.info("GET request for " + requestKey + " yielded value: " + requestValue);
        return new KVMessage(
            requestKey,
            requestValue,
            request.getClientId(),
            KVMessage.StatusType.GET_SUCCESS,
            request.getRequestId());
      } else {
        logger.info("GET request for " + requestKey + " failed.");
        return new KVMessage(
            requestKey,
            null,
            request.getClientId(),
            KVMessage.StatusType.GET_ERROR,
            request.getRequestId());
      }
    } catch (FileNotFoundException e) {
      logger.error("No storage file exists for GET operation", e);
      return new KVMessage(
          requestKey,
          requestValue,
          request.getClientId(),
          KVMessage.StatusType.GET_ERROR,
          request.getRequestId());
    } catch (IOException e) {
      logger.error(" I/O error on working with the storage file during GET operation", e);
      return new KVMessage(
          requestKey,
          requestValue,
          request.getClientId(),
          KVMessage.StatusType.GET_ERROR,
          request.getRequestId());
    } catch (Exception e) {
      logger.error("Something went wrong during GET operation", e);
      return new KVMessage(
          requestKey,
          requestValue,
          request.getClientId(),
          KVMessage.StatusType.GET_ERROR,
          request.getRequestId());
    }
  }

  public DiskStorageWriteResponse write(final KVMessage request, StorageType storageType) {
    assert (request != null
        && (request.getStatus() == PUT
            || request.getStatus() == SUBSCRIBE
            || request.getStatus() == UNSUBSCRIBE));
    logger.info("Received request " + request + " with storage type " + storageType);
    try {
      boolean checks = Verifier.verifyKVMessageMAC(request) && !seenIDs.contains(request.getMAC());
      if (!checks) {
        logger.error("Verification failed for the KVMessage " + request);
        return new DiskStorageWriteResponse(
            new KVMessage(
                request.getKey(),
                request.getValue(),
                request.getClientId(),
                KVMessage.StatusType.AUTH_FAILED,
                request.getRequestId()),
            null);
      }
    } catch (EncryptionException e) {
      logger.error("Verification failed for the KVMessage " + request, e);
      return new DiskStorageWriteResponse(
          new KVMessage(
              request.getKey(),
              request.getValue(),
              request.getClientId(),
              KVMessage.StatusType.AUTH_FAILED,
              request.getRequestId()),
          null);
    }

    logger.info(
        "PUT request for "
            + request.getKey()
            + ": "
            + request.getValue()
            + "for: "
            + storageType.name());

    synchronized (diskWriteLock) {
      File workingFile = correctFileBasedOnEnum(storageType);
      String requestKey = request.getKey();
      String requestValue = request.getValue();
      KVMessage.StatusType status = KVMessage.StatusType.PUT_ERROR;

      final File newWorkingFile = new File("temp_" + uniqueID + "_" + storageType.name() + ".txt");
      BufferedWriter newFileWriter;
      BufferedReader oldFileReader;
      KVMessage kvMessageResponse;
      ArrayList<UUID> subscribers = null;
      try {
        newFileWriter = new BufferedWriter(new FileWriter(newWorkingFile), 16384);
        oldFileReader = new BufferedReader(new FileReader(workingFile), 16384);
        String entry;
        boolean found = false;
        // get storage
        while ((entry = oldFileReader.readLine()) != null) {

          StorageUnit currentUnit = StorageUnit.deserialize(entry.trim(), encryption);
          String key = currentUnit.key;

          if (key.equals(requestKey)) {
            found = true;
            switch (request.getStatus()) {
              case SUBSCRIBE:
                assert (request.getClientId() != null);
                currentUnit.subscribers.add(request.getClientId());
                newFileWriter.write(currentUnit.serialize(encryption));
                newFileWriter.newLine();
                status = SUBSCRIBE_SUCCESS;
                break;
              case UNSUBSCRIBE:
                assert (request.getClientId() != null);
                currentUnit.subscribers.remove(request.getClientId());
                newFileWriter.write(currentUnit.serialize(encryption));
                newFileWriter.newLine();
                status = UNSUBSCRIBE_SUCCESS;
                break;
              case PUT:
                assert (subscribers == null);
                subscribers = currentUnit.subscribers;
                if (requestValue != null) {
                  currentUnit.value = requestValue;
                  currentUnit.uniqueID = request.getUniqueID();
                  currentUnit.MAC = request.getMAC();
                  newFileWriter.write(currentUnit.serialize(encryption));
                  newFileWriter.newLine();
                  status = KVMessage.StatusType.PUT_UPDATE;
                } else {
                  status = KVMessage.StatusType.DELETE_SUCCESS;
                  seenIDs.add(currentUnit.MAC);
                }
                break;
            }

          } else {
            newFileWriter.write(entry.trim());
            newFileWriter.newLine();
          }
        }

        if (!found) {
          switch (request.getStatus()) {
            case SUBSCRIBE:
              status = SUBSCRIBE_ERROR;
              break;
            case UNSUBSCRIBE:
              status = UNSUBSCRIBE_ERROR;
              break;
            case PUT:
              if (requestValue != null) {
                StorageUnit currentUnit =
                    new StorageUnit(
                        requestKey, requestValue, request.getUniqueID(), request.getMAC());
                newFileWriter.write(currentUnit.serialize(encryption));
                newFileWriter.newLine();
                status = KVMessage.StatusType.PUT_SUCCESS;
                seenIDs.add(currentUnit.MAC);
              } else {
                status = KVMessage.StatusType.DELETE_ERROR;
              }
              break;
          }
        }

        oldFileReader.close();
        newFileWriter.flush();
        newFileWriter.close();

        Files.move(
            newWorkingFile.toPath(), workingFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
        kvMessageResponse =
            new KVMessage(
                requestKey, requestValue, request.getClientId(), status, request.getRequestId());
      } catch (FileNotFoundException e) {
        logger.error("No storage file exists for operation " + request, e);
        kvMessageResponse =
            new KVMessage(
                requestKey,
                requestValue,
                request.getClientId(),
                correspondingErrorStatus(request.getStatus()),
                request.getRequestId());
      } catch (SecurityException e) {
        logger.error("Security rules do not allow file deletion or renaming", e);
        kvMessageResponse =
            new KVMessage(
                requestKey,
                requestValue,
                request.getClientId(),
                correspondingErrorStatus(request.getStatus()),
                request.getRequestId());
      } catch (IOException e) {
        logger.error("I/O error on working with the storage file during operation: " + request, e);
        kvMessageResponse =
            new KVMessage(
                requestKey,
                requestValue,
                request.getClientId(),
                correspondingErrorStatus(request.getStatus()),
                request.getRequestId());
      } catch (Exception e) {
        logger.error("Something went wrong during operation: " + request, e);
        kvMessageResponse =
            new KVMessage(
                requestKey,
                requestValue,
                request.getClientId(),
                correspondingErrorStatus(request.getStatus()),
                request.getRequestId());
      }
      DiskStorageWriteResponse response =
          new DiskStorageWriteResponse(kvMessageResponse, subscribers);
      logger.info("Response: " + response);
      return new DiskStorageWriteResponse(kvMessageResponse, subscribers);
    }
  }

  private KVMessage.StatusType correspondingErrorStatus(KVMessage.StatusType requestStatus) {
    switch (requestStatus) {
      case PUT:
        return PUT_ERROR;
      case SUBSCRIBE:
        return SUBSCRIBE_ERROR;
      case UNSUBSCRIBE:
        return UNSUBSCRIBE_ERROR;
      default:
        throw new AssertionError("Unexpected type " + requestStatus);
    }
  }

  public KVMessage.StatusType putStorageUnit(
      final StorageUnit storageUnit, StorageType storageType) {
    assert (storageUnit != null);
    assert (storageUnit.value != null);

    try {
      if (!Verifier.verifyStorageUnitMAC(storageUnit) || seenIDs.contains(storageUnit.uniqueID)) {
        return KVMessage.StatusType.AUTH_FAILED;
      }
    } catch (EncryptionException e) {
      logger.error("Verification failed for the StorageUnit " + storageUnit.toString(), e);
      return KVMessage.StatusType.AUTH_FAILED;
    }

    logger.info(
        "PUT request for "
            + storageUnit.key
            + ": "
            + storageUnit.value
            + "for: "
            + storageType.name());

    synchronized (diskWriteLock) {
      File workingFile = correctFileBasedOnEnum(storageType);
      String requestKey = storageUnit.key;
      KVMessage.StatusType status = KVMessage.StatusType.PUT_ERROR;

      final File newWorkingFile = new File("temp_" + uniqueID + "_" + storageType.name() + ".txt");
      BufferedWriter newFileWriter;
      BufferedReader oldFileReader;
      try {
        newFileWriter = new BufferedWriter(new FileWriter(newWorkingFile), 16384);
        oldFileReader = new BufferedReader(new FileReader(workingFile), 16384);
        String entry;
        boolean found = false;
        // get storage
        while ((entry = oldFileReader.readLine()) != null) {

          StorageUnit currentUnit = StorageUnit.deserialize(entry.trim(), encryption);
          String key = currentUnit.key;

          if (key.equals(requestKey)) {
            found = true;
            currentUnit = storageUnit;
            newFileWriter.write(currentUnit.serialize(encryption));
            newFileWriter.newLine();
            status = KVMessage.StatusType.PUT_UPDATE;
            seenIDs.add(storageUnit.MAC);
          } else {
            newFileWriter.write(entry.trim());
            newFileWriter.newLine();
          }
        }
        if (!found) {
          newFileWriter.write(storageUnit.serialize(encryption));
          newFileWriter.newLine();
          status = KVMessage.StatusType.PUT_SUCCESS;
          seenIDs.add(storageUnit.MAC);
        }
        oldFileReader.close();
        newFileWriter.flush();
        newFileWriter.close();

        Files.move(
            newWorkingFile.toPath(), workingFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
        return status;
      } catch (Exception e) {
        logger.error("Something went wrong during PUT operation", e);
        return KVMessage.StatusType.PUT_ERROR;
      }
    }
  }

  public DataTransferMessage partitionDatabaseAndGetKeysInRange(
      ECSMessage ecsMessage,
      final String[] hashRange,
      StorageType storageType,
      boolean deleteKeysDuringPartition) {
    HashSet<StorageUnit> dataToTransfer = new HashSet<>();

    synchronized (diskWriteLock) {
      File workingFile = correctFileBasedOnEnum(storageType);

      final File newWorkingFile =
          new File("temp_" + uniqueID + "_" + storageType.name() + "_partitioning" + ".txt");
      BufferedWriter newFileWriter;
      BufferedReader oldFileReader;
      try {
        newFileWriter = new BufferedWriter(new FileWriter(newWorkingFile));
        oldFileReader = new BufferedReader(new FileReader(workingFile));
        String entry;

        while ((entry = oldFileReader.readLine()) != null) {
          StorageUnit currentUnit = StorageUnit.deserialize(entry.trim(), encryption);

          if (ECSUtils.checkIfKeyBelongsInRange(currentUnit.key, hashRange)) {
            dataToTransfer.add(currentUnit);
            if (!deleteKeysDuringPartition) {
              newFileWriter.write(entry.trim());
              newFileWriter.newLine();
            }
          } else {
            newFileWriter.write(entry.trim());
            newFileWriter.newLine();
          }
        }

        oldFileReader.close();
        newFileWriter.flush();
        newFileWriter.close();
        Files.move(
            newWorkingFile.toPath(), workingFile.toPath(), StandardCopyOption.REPLACE_EXISTING);

        return new DataTransferMessage(
            DATA_TRANSFER_REQUEST,
            dataToTransfer,
            "Partition Successful, Payload Ready",
            ecsMessage);

      } catch (FileNotFoundException e) {
        logger.error("No storage file exists for partition operation", e);
        return new DataTransferMessage(DATA_TRANSFER_FAILURE, e.toString(), ecsMessage);
      } catch (SecurityException e) {
        logger.error("Security rules do not allow file deletion or renaming", e);
        return new DataTransferMessage(DATA_TRANSFER_FAILURE, e.toString(), ecsMessage);
      } catch (IOException e) {
        logger.error("I/O error on working with the storage file during PUT operation", e);
        return new DataTransferMessage(DATA_TRANSFER_FAILURE, e.toString(), ecsMessage);
      } catch (Exception e) {
        logger.error("Something went wrong during database partitioning", e);
        return new DataTransferMessage(DATA_TRANSFER_FAILURE, e.toString(), ecsMessage);
      }
    }
  }

  public DataTransferMessage updateDatabaseWithKVDataTransfer(
      final DataTransferMessage dataTransferMessage, StorageType storageType) {
    HashSet<StorageUnit> dataToWrite = dataTransferMessage.getPayload();

    File workingFile = correctFileBasedOnEnum(storageType);
    BufferedWriter databaseFileWriter;
    try {
      databaseFileWriter = new BufferedWriter(new FileWriter(workingFile, true));

      for (StorageUnit storageUnit : dataToWrite) {
        assert (storageUnit != null);
        assert (storageUnit.value != null);
        try {
          assert (Verifier.verifyStorageUnitMAC(storageUnit) && !seenIDs.contains(storageUnit.MAC));
        } catch (EncryptionException e) {
          logger.error("Verification failed for the StorageUnit " + storageUnit.toString(), e);
          return new DataTransferMessage(
              DATA_TRANSFER_FAILURE, e.toString(), dataTransferMessage.getECSMessage());
        }
        databaseFileWriter.write(storageUnit.serialize(encryption));
        databaseFileWriter.newLine();
        seenIDs.add(storageUnit.MAC);
      }

      databaseFileWriter.close();
      return new DataTransferMessage(
          DATA_TRANSFER_SUCCESS, "Added new keys to database", dataTransferMessage.getECSMessage());

    } catch (FileNotFoundException e) {
      logger.error("No storage file exists for partition operation", e);
      return new DataTransferMessage(
          DATA_TRANSFER_FAILURE, e.toString(), dataTransferMessage.getECSMessage());
    } catch (SecurityException e) {
      logger.error("Security rules do not allow file deletion or renaming", e);
      return new DataTransferMessage(
          DATA_TRANSFER_FAILURE, e.toString(), dataTransferMessage.getECSMessage());
    } catch (IOException e) {
      logger.error("I/O error on working with the storage file during PUT operation", e);
      return new DataTransferMessage(
          DATA_TRANSFER_FAILURE, e.toString(), dataTransferMessage.getECSMessage());
    } catch (Exception e) {
      logger.error("Something went wrong during database partitioning", e);
      return new DataTransferMessage(
          DATA_TRANSFER_FAILURE, e.toString(), dataTransferMessage.getECSMessage());
    }
  }

  private File correctFileBasedOnEnum(StorageType type) {
    switch (type) {
      case REPLICA_1:
        return this.replica1File;
      case REPLICA_2:
        return this.replica2File;
      default:
        return this.storageFile;
    }
  }

  public enum StorageType {
    SELF,
    REPLICA_1,
    REPLICA_2
  }

  public static class DiskStorageWriteResponse {
    private final KVMessage kvMessageResponse;
    private final ArrayList<UUID> subscribers;

    public DiskStorageWriteResponse(KVMessage kvMessageResponse, ArrayList<UUID> subscribers) {
      this.kvMessageResponse = kvMessageResponse;
      this.subscribers = subscribers;
    }

    public KVMessage getKvMessageResponse() {
      return kvMessageResponse;
    }

    public ArrayList<UUID> getSubscribers() {
      return subscribers;
    }

    @Override
    public String toString() {
      return "DiskStorageWriteResponse{"
          + "kvMessage="
          + kvMessageResponse
          + ",  subscribers="
          + subscribers
          + "}";
    }
  }
}
