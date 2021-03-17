package shared.communication.messages;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import java.nio.charset.StandardCharsets;
import org.apache.log4j.Logger;

public abstract class Message {
  private static final Logger logger = Logger.getLogger(Message.class);

  public static Message deserialize(byte[] bytes) throws MessageException {
    String messageJson = new String(bytes, StandardCharsets.UTF_8).trim();
    GsonBuilder builder = new GsonBuilder();
    builder.registerTypeAdapter(Message.class, new GsonInterfaceAdapter());
    Gson gson = builder.create();

    try {
      return gson.fromJson(messageJson, Message.class);
    } catch (JsonSyntaxException e) {
      logger.error("Message JSON is invalid! \n" + messageJson);
      throw new MessageException("deserialization failed for " + messageJson);
    }
  }

  /** @return a byte array ready for transporting over the network. */
  public byte[] serialize() {
    GsonBuilder builder = new GsonBuilder();
    builder.registerTypeAdapter(Message.class, new GsonInterfaceAdapter());
    Gson gson = builder.create();
    String messageJson = gson.toJson(this, Message.class);
    return messageJson.getBytes(StandardCharsets.UTF_8);
  }

  public enum StatusType {
    GET, /* Get - request */
    GET_ERROR, /* requested tuple (i.e. value) not found */
    GET_SUCCESS, /* requested tuple (i.e. value) found */
    PUT, /* Put - request */
    PUT_SUCCESS, /* Put - request successful, tuple inserted */
    PUT_UPDATE, /* Put - request successful, i.e. value updated */
    PUT_ERROR, /* Put - request not successful */
    DELETE_SUCCESS, /* Delete - request successful */
    DELETE_ERROR, /* Delete - request not successful */
    FAILED,

    NOT_RESPONSIBLE,
    SERVER_WRITE_LOCK,
    SERVER_STOPPED
  }
}
