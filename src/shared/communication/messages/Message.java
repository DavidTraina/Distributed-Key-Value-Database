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
}
