package shared.messages;

public interface KVMessage {

  /** @return the key that is associated with this message, null if not key is associated. */
  String getKey();

  /** @return the value that is associated with this message, null if not value is associated. */
  String getValue();

  /**
   * @return a status string that is used to identify request types, response types and error types
   *     associated to the message.
   */
  StatusType getStatus();

  enum StatusType {
    GET, /* Get - request */
    GET_ERROR, /* requested tuple (i.e. value) not found */
    GET_SUCCESS, /* requested tuple (i.e. value) found */
    PUT, /* Put - request */
    PUT_SUCCESS, /* Put - request successful, tuple inserted */
    PUT_UPDATE, /* Put - request successful, i.e. value updated */
    PUT_ERROR, /* Put - request not successful */
    DELETE_SUCCESS, /* Delete - request successful */
    DELETE_ERROR, /* Delete - request successful */
  }
}
