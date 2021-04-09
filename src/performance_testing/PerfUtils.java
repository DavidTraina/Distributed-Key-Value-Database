package performance_testing;

import java.io.*;
import java.security.SecureRandom;
import java.util.*;
import java.util.stream.Collectors;

public class PerfUtils {

  public static HashMap<String, String> loadEnron(String path, int numRequests) throws IOException {
    final File folder = new File(path);
    HashMap<String, String> emails = new HashMap<>();
    loadAllFilesInFolder(folder, emails, numRequests);
    return emails;
  }

  private static int loadAllFilesInFolder(
      File folder, HashMap<String, String> emails, int numRequests) throws IOException {
    int remaining = numRequests;
    for (final File file : folder.listFiles()) {
      if (remaining > 0) {
        if (file.isDirectory()) {
          remaining = loadAllFilesInFolder(file, emails, remaining);
        } else {
          remaining--;
          BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
          String firstLine = bufferedReader.readLine();
          String messageID = firstLine.split("Message-ID: ", 2)[1];
          if (messageID.length() > 20) messageID = messageID.substring(0, 19);
          String subject = null;
          String entry;
          while ((entry = bufferedReader.readLine()) != null) {
            if (entry.contains("Subject: ")) {
              subject = entry.split("Subject: ", 2)[1];
              if (subject.length() > 120 * 1024) subject = subject.substring(0, 120 * 1024 - 1);
              break;
            }
          }
          emails.put(messageID, subject);
        }
      }
    }
    return remaining;
  }

  public static ArrayList<HashMap<String, String>> split(int clients, HashMap<String, String> kv) {
    ArrayList<HashMap<String, String>> emailsPerClient = new ArrayList<>();
    for (int i = 0; i < clients; i++) {
      emailsPerClient.add(new HashMap<>());
    }
    int i = 0;
    for (Map.Entry<String, String> entry : kv.entrySet()) {
      emailsPerClient.get(i % clients).put(entry.getKey(), entry.getValue());
      emailsPerClient.get(clients - 1 - i % clients).put(entry.getKey(), entry.getValue());
      i++;
      if (i * 2 > kv.size()) break;
    }
    return emailsPerClient;
  }

  public static HashMap<String, String> createRandom(int numRequests) {
    Random random = new Random();
    String value, key;
    HashMap<String, String> pairs = new HashMap<>();
    int numRequestsCompleted = 0;

    while (numRequestsCompleted != numRequests) {
      // Choose a small key to have collisions assuming high numRequests
      key = createRandomCode(3, "ABCDEF");
      // generate a random integer between 0 and 1000
      int valueLen = random.nextInt(1000);
      value = createRandomCode(valueLen, "ABCDEFGHIJKLMNOPQRSTUVWXYZ");

      if (value.length() == 0) {
        value = null;
      }
      pairs.put(key, value);
      numRequestsCompleted += 1;
    }
    return pairs;
  }

  // Copied from stackoverflow:
  // https://stackoverflow.com/questions/39222044/generate-random-string-in-java
  private static String createRandomCode(int codeLength, String id) {
    return new SecureRandom()
        .ints(codeLength, 0, id.length())
        .mapToObj(id::charAt)
        .map(Object::toString)
        .collect(Collectors.joining());
  }
}
