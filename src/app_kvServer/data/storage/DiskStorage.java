package app_kvServer.data.storage;

import static shared.communication.messages.DataTransferMessage.DataTransferMessageType.DATA_TRANSFER_FAILURE;
import static shared.communication.messages.DataTransferMessage.DataTransferMessageType.DATA_TRANSFER_REQUEST;
import static shared.communication.messages.DataTransferMessage.DataTransferMessageType.DATA_TRANSFER_SUCCESS;

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;
import shared.communication.messages.DataTransferMessage;
import shared.communication.messages.KVMessage;

public class DiskStorage {
  private static final Logger logger = Logger.getLogger(DiskStorage.class);
  private final File storageFile;
  private final File replica1File;
  private final File replica2File;
  private final String uniqueID;

  public DiskStorage(final String uniqueID) throws DiskStorageException {
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
    logger.info("GET request for " + request.getKey());

    File workingFile = correctFileBasedOnEnum(storageType);
    String requestKey = request.getKey();
    assert (request.getValue() == null);
    String requestValue = null;

    BufferedReader reader;
    try {
      reader = new BufferedReader(new FileReader(workingFile), 16384);
      String entry;

      while ((entry = reader.readLine()) != null) {
        String[] pair = entry.trim().split("\\s+", 2); // We assume keys/values have no spaces

        String key = pair[0].trim();
        if (key.equals(requestKey)) {
          requestValue = pair[1].trim();
          break;
        }
      }
      reader.close();
      if (requestValue != null) {
        logger.info("GET request for " + requestKey + " yielded value: " + requestValue);
        return new KVMessage(requestKey, requestValue, KVMessage.StatusType.GET_SUCCESS);
      } else {
        logger.info("GET request for " + requestKey + " failed.");
        return new KVMessage(requestKey, null, KVMessage.StatusType.GET_ERROR);
      }
    } catch (FileNotFoundException e) {
      logger.error("No storage file exists for GET operation", e);
      return new KVMessage(requestKey, requestValue, KVMessage.StatusType.GET_ERROR);
    } catch (IOException e) {
      logger.error(" I/O error on working with the storage file during GET operation", e);
      return new KVMessage(requestKey, requestValue, KVMessage.StatusType.GET_ERROR);
    } catch (Exception e) {
      logger.error("Something went wrong during GET operation", e);
      return new KVMessage(requestKey, requestValue, KVMessage.StatusType.GET_ERROR);
    }
  }

  public KVMessage put(final KVMessage request, StorageType storageType) {
    assert (request != null);
    logger.info("PUT request for " + request.getKey() + ": " + request.getValue());

    File workingFile = correctFileBasedOnEnum(storageType);
    String requestKey = request.getKey();
    String requestValue = request.getValue();
    KVMessage.StatusType status = KVMessage.StatusType.PUT_ERROR;

    final File newWorkingFile = new File("temp_" + uniqueID + "_" + storageType.name() + ".txt");
    BufferedWriter newFileWriter;
    BufferedReader oldFileReader;
    try {
      newFileWriter = new BufferedWriter(new FileWriter(newWorkingFile), 16384);
      oldFileReader = new BufferedReader(new FileReader(storageFile), 16384);
      String entry;
      boolean found = false;
      // get storage
      while ((entry = oldFileReader.readLine()) != null) {
        List<String> pair =
            Splitter.on(CharMatcher.whitespace()).limit(2).splitToList(entry.trim());

        String key = pair.get(0).trim();
        String value = pair.get(1).trim();
        if (key.equals(requestKey)) {
          found = true;
          if (requestValue != null) {
            newFileWriter.write(requestKey + " " + requestValue);
            newFileWriter.newLine();
            status = KVMessage.StatusType.PUT_UPDATE;
          } else {
            status = KVMessage.StatusType.DELETE_SUCCESS;
          }
        } else {
          newFileWriter.write(key + " " + value);
          newFileWriter.newLine();
        }
      }
      if (!found) {
        if (requestValue != null) {
          newFileWriter.write(requestKey + " " + requestValue);
          newFileWriter.newLine();
          status = KVMessage.StatusType.PUT_SUCCESS;
        } else {
          status = KVMessage.StatusType.DELETE_ERROR;
        }
      }
      oldFileReader.close();
      newFileWriter.flush();
      newFileWriter.close();

      Files.move(
          newWorkingFile.toPath(), workingFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
      return new KVMessage(requestKey, requestValue, status);
    } catch (FileNotFoundException e) {
      logger.error("No storage file exists for PUT operation", e);
      return new KVMessage(requestKey, requestValue, KVMessage.StatusType.PUT_ERROR);
    } catch (SecurityException e) {
      logger.error("Security rules do not allow file deletion or renaming", e);
      return new KVMessage(requestKey, requestValue, KVMessage.StatusType.PUT_ERROR);
    } catch (IOException e) {
      logger.error("I/O error on working with the storage file during PUT operation", e);
      return new KVMessage(requestKey, requestValue, KVMessage.StatusType.PUT_ERROR);
    } catch (Exception e) {
      logger.error("Something went wrong during PUT operation", e);
      return new KVMessage(requestKey, requestValue, KVMessage.StatusType.PUT_ERROR);
    }
  }

  public DataTransferMessage partitionDatabaseAndGetKeysInRange(
      final String[] hashRange, StorageType storageType) {
    HashMap<String, String> dataToTransfer = new HashMap<>();

    File workingFile = correctFileBasedOnEnum(storageType);

    final File newWorkingFile = new File("temp_" + uniqueID + "_" + storageType.name() + ".txt");
    BufferedWriter newFileWriter;
    BufferedReader oldFileReader;
    try {
      newFileWriter = new BufferedWriter(new FileWriter(newWorkingFile));
      oldFileReader = new BufferedReader(new FileReader(workingFile));
      String entry;

      while ((entry = oldFileReader.readLine()) != null) {
        List<String> pair =
            Splitter.on(CharMatcher.whitespace()).limit(2).splitToList(entry.trim());

        String key = pair.get(0).trim();
        String value = pair.get(1).trim();

        if (ECSUtils.checkIfKeyBelongsInRange(key, hashRange)) {
          dataToTransfer.put(key, value);
        } else {
          newFileWriter.write(key + " " + value);
          newFileWriter.newLine();
        }
      }

      oldFileReader.close();
      newFileWriter.flush();
      newFileWriter.close();
      Files.move(
          newWorkingFile.toPath(), workingFile.toPath(), StandardCopyOption.REPLACE_EXISTING);

      return new DataTransferMessage(
          DATA_TRANSFER_REQUEST, dataToTransfer, "Partition Successful, Payload Ready");

    } catch (FileNotFoundException e) {
      logger.error("No storage file exists for partition operation", e);
      return new DataTransferMessage(DATA_TRANSFER_FAILURE, e.toString());
    } catch (SecurityException e) {
      logger.error("Security rules do not allow file deletion or renaming", e);
      return new DataTransferMessage(DATA_TRANSFER_FAILURE, e.toString());
    } catch (IOException e) {
      logger.error("I/O error on working with the storage file during PUT operation", e);
      return new DataTransferMessage(DATA_TRANSFER_FAILURE, e.toString());
    } catch (Exception e) {
      logger.error("Something went wrong during database partitioning", e);
      return new DataTransferMessage(DATA_TRANSFER_FAILURE, e.toString());
    }
  }

  public DataTransferMessage updateDatabaseWithKVDataTransfer(
      final DataTransferMessage dataTransferMessage, StorageType storageType) {
    HashMap<String, String> dataToWrite = dataTransferMessage.getPayload();

    File workingFile = correctFileBasedOnEnum(storageType);
    BufferedWriter databaseFileWriter;
    try {
      databaseFileWriter = new BufferedWriter(new FileWriter(workingFile, true));

      for (Map.Entry<String, String> entry : dataToWrite.entrySet()) {
        databaseFileWriter.write(entry.getKey() + " " + entry.getValue());
        databaseFileWriter.newLine();
      }

      databaseFileWriter.close();
      return new DataTransferMessage(DATA_TRANSFER_SUCCESS, "Added new keys to database");

    } catch (FileNotFoundException e) {
      logger.error("No storage file exists for partition operation", e);
      return new DataTransferMessage(DATA_TRANSFER_FAILURE, e.toString());
    } catch (SecurityException e) {
      logger.error("Security rules do not allow file deletion or renaming", e);
      return new DataTransferMessage(DATA_TRANSFER_FAILURE, e.toString());
    } catch (IOException e) {
      logger.error("I/O error on working with the storage file during PUT operation", e);
      return new DataTransferMessage(DATA_TRANSFER_FAILURE, e.toString());
    } catch (Exception e) {
      logger.error("Something went wrong during database partitioning", e);
      return new DataTransferMessage(DATA_TRANSFER_FAILURE, e.toString());
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
}
