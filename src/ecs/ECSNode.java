package ecs;

public class ECSNode {
  private final String nodeHash;
  private final String name;
  private final String address;
  private final Integer port;
  private String lowerRange;

  public ECSNode(String name, String address, Integer port) {
    this.name = name;
    this.address = address;
    this.port = port;
    this.nodeHash = ECSUtils.calculateMD5Hash(name);
  }

  public ECSNode(String address, Integer port) {
    this.name = address + ":" + port;
    this.address = address;
    this.port = port;
    this.nodeHash = ECSUtils.calculateMD5Hash(name);
  }

  /** @return the name of the node (ie "Server 8.8.8.8") */
  public String getNodeName() {
    return name;
  }

  /** @return the hash of the node (ie "Server 8.8.8.8") */
  public String getNodeHash() {
    return nodeHash;
  }

  /** @return the hostname of the node (ie "8.8.8.8") */
  public String getNodeHost() {
    return address;
  }

  /** @return the port number of the node (ie 8080) */
  public int getNodePort() {
    return port;
  }

  /**
   * @return array of two strings representing the low and high range of the hashes that the given
   *     node is responsible for
   */
  public String[] getNodeHashRange() {
    return new String[] {lowerRange, nodeHash};
  }

  /**
   * sets hash range to an array of two strings representing the low and high range of the hashes
   * that the given node is responsible for
   */
  public void setLowerRange(String lowerRange) {
    this.lowerRange = lowerRange;
  }

  /**
   * sets hash range to an array of two strings representing the low and high range of the hashes
   * that the given node is responsible for
   */
  public String getLowerRange() {
    return lowerRange;
  }
}
