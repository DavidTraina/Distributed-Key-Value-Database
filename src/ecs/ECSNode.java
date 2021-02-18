package ecs;

public class ECSNode {
  private final String name;
  private final String address;
  private final Integer port;
  private String[] hashRanges;

  public ECSNode(String name, String address, Integer port) {
    this.name = name;
    this.address = address;
    this.port = port;
  }

  /** @return the name of the node (ie "Server 8.8.8.8") */
  public String getNodeName() {
    return name;
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
   * sets hash range to an array of two strings representing the low and high range of the hashes
   * that the given node is responsible for
   */
  public void setNodeHashRange(String[] hashRanges) {
    this.hashRanges = hashRanges;
  }

  /**
   * @return array of two strings representing the low and high range of the hashes that the given
   *     node is responsible for
   */
  public String[] getNodeHashRange() {
    return hashRanges;
  }
}
