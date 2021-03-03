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
  private static final int MAX_CREATION_ATTEMPTS = 5;
  private final File storageFile;
  private final String uniqueID;

  public DiskStorage(final String uniqueID) throws DiskStorageException {
    this.uniqueID = uniqueID;
    String fileName = "KeyValueData_" + uniqueID + ".txt";
    this.storageFile = new File(fileName);
    // remove existing storage to start fresh
    if (storageFile.exists() && !storageFile.delete()) {
      throw new DiskStorageException("Unable to delete file " + storageFile.getAbsolutePath());
    }
    int creationAttempts = 0;
    while (true) {
      try {
        if (storageFile.createNewFile()) {
          logger.info("Storage file " + storageFile.getAbsolutePath() + " created.");
        } else {
          logger.info(
              "Storage file "
                  + storageFile.getAbsolutePath()
                  + " already exists. The server will be using existing database.");
        }
        break;
      } catch (IOException e) {
        if (++creationAttempts == MAX_CREATION_ATTEMPTS) {
          logger.error(
              "Could not establish connection to disk storage after "
                  + creationAttempts
                  + " attempts.");
          throw new DiskStorageException(e.getLocalizedMessage());
        }
        logger.warn(
            "Attempt number "
                + creationAttempts
                + " to connect to disk storage failed. Retrying...");
      }
    }
  }

  public KVMessage get(final KVMessage request) {
    assert (request != null);
    logger.info("GET request for " + request.getKey());

    String requestKey = request.getKey();
    assert (request.getValue() == null);
    String requestValue = null;

    BufferedReader reader;
    try {
      reader = new BufferedReader(new FileReader(storageFile), 16384);
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

  public KVMessage put(final KVMessage request) {
    assert (request != null);
    logger.info("PUT request for " + request.getKey() + ": " + request.getValue());

    String requestKey = request.getKey();
    String requestValue = request.getValue();
    KVMessage.StatusType status = KVMessage.StatusType.PUT_ERROR;

    final File newStorageFile = new File("temp_" + uniqueID + ".txt");
    BufferedWriter newFileWriter;
    BufferedReader oldFileReader;
    try {
      newFileWriter = new BufferedWriter(new FileWriter(newStorageFile), 16384);
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
          newStorageFile.toPath(), storageFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
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

  public DataTransferMessage partitionDatabaseAndGetKeysInRange(final String[] hashRange) {
    HashMap<String, String> dataToTransfer = new HashMap<>();

    final File newStorageFile = new File("temp.txt");
    BufferedWriter newFileWriter;
    BufferedReader oldFileReader;
    try {
      newFileWriter = new BufferedWriter(new FileWriter(newStorageFile));
      oldFileReader = new BufferedReader(new FileReader(storageFile));
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
          newStorageFile.toPath(), storageFile.toPath(), StandardCopyOption.REPLACE_EXISTING);

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
      final DataTransferMessage dataTransferMessage) {
    HashMap<String, String> dataToWrite = dataTransferMessage.getPayload();

    BufferedWriter databaseFileWriter;
    try {
      databaseFileWriter = new BufferedWriter(new FileWriter(storageFile, true));

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
}
