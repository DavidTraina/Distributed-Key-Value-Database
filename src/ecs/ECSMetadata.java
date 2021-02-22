package ecs;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import javax.xml.bind.DatatypeConverter;

public class ECSMetadata {
  private ArrayList<ECSNode> serverData;
  private MessageDigest md;

  public ECSMetadata(ArrayList<ECSNode> serverData) {
    this.serverData = serverData;
    try {
      this.md = MessageDigest.getInstance("MD5");
    } catch (NoSuchAlgorithmException e) {
      e.printStackTrace();
    }
  }

  public void clear() {
    this.serverData.clear();
  }

  public ArrayList<ECSNode> getMetadata() {
    return serverData;
  }

  public ECSNode getNodeBasedOnKey(String key) {
    md.update(key.getBytes());
    byte[] digest = md.digest();
    String hash = DatatypeConverter.printHexBinary(digest).toUpperCase();
    for (int i = 0; i < serverData.size(); i++) {
      String[] ends = serverData.get(i).getNodeHashRange();
      // the node is not wrapping
      if (ends[0].compareTo(ends[1]) < 0) {
        if (ends[0].compareTo(hash) <= 0 && ends[1].compareTo(hash) > 0) {
          return serverData.get(i);
        }
      } else {
        if (ends[1].compareTo(hash) <= 0 || ends[0].compareTo(hash) > 0) {
          return serverData.get(i);
        }
      }
    }
    System.out.println(
        "This should not be happening. This means that we could not find right node");
    return null;
  }
}
