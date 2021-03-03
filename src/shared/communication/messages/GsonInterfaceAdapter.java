package shared.communication.messages;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import java.lang.reflect.Type;

/*
    Class to handle serialization/deserialization for an interface or abstract class
**/
public class GsonInterfaceAdapter implements JsonSerializer, JsonDeserializer {

  private static final String CLASSNAME = "CLASSNAME";
  private static final String DATA = "DATA";

  public Message deserialize(
      JsonElement jsonElement, Type type, JsonDeserializationContext jsonDeserializationContext)
      throws JsonParseException {

    JsonObject jsonObject = jsonElement.getAsJsonObject();
    JsonPrimitive prim = (JsonPrimitive) jsonObject.get(CLASSNAME);
    String className = prim.getAsString();
    Class klass = getObjectClass(className);
    return jsonDeserializationContext.deserialize(jsonObject.get(DATA), klass);
  }

  public JsonElement serialize(
      Object o, Type type, JsonSerializationContext jsonSerializationContext) {
    JsonObject jsonObject = new JsonObject();
    jsonObject.addProperty(CLASSNAME, o.getClass().getName());
    jsonObject.add(DATA, jsonSerializationContext.serialize(o));
    return jsonObject;
  }

  /** *** Helper method to get the className of the object to be deserialized **** */
  public Class getObjectClass(String className) {
    try {
      return Class.forName(className);
    } catch (ClassNotFoundException e) {
      throw new JsonParseException(e.getMessage());
    }
  }
}
