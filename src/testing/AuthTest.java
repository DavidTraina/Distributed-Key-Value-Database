package testing;

import static org.junit.Assert.*;

import client.KVStore;
import client.KVStoreException;
import com.google.gson.Gson;
import ecs.ECSNode;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.concurrent.TimeUnit;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import shared.communication.Protocol;
import shared.communication.ProtocolException;
import shared.communication.messages.KVMessage;
import shared.communication.security.*;
import shared.communication.security.encryption.Encryption;
import shared.communication.security.encryption.EncryptionException;
import shared.communication.security.keys.ClientPublicKey;
import shared.communication.security.property_stores.ClientPropertyStore;
import shared.communication.security.property_stores.ServerPropertyStore;

public class AuthTest {

  private static Process server;

  @BeforeClass
  public static void startServer() {
    try {
      ProcessBuilder builder =
          new ProcessBuilder("java", "-jar", "m2-server.jar", "50001", "0", "LRU");
      builder.redirectOutput(new File("/dev/null"));
      builder.redirectError(new File("/dev/null"));
      server = builder.start();

      TimeUnit.MILLISECONDS.sleep(100); // Wait for server to start properly
      KVStore kvStore = new KVStore(InetAddress.getLocalHost(), 50001);

      // Loop to ensure server is up before starting tests
      while (true) {
        try {
          kvStore.connect();
          break;
        } catch (KVStoreException e) {
          TimeUnit.MILLISECONDS.sleep(100);
        }
      }

      ClientPropertyStore.getInstance().setSenderID("client");
      // Private key is init in KvStore i.e BeforeClass
      ServerPropertyStore.getInstance()
          .setClientPublicKey(KeyLoader.getPublicKey(ClientPublicKey.base64EncodedPublicKey));
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @AfterClass
  public static void stopServer() {
    server.destroy();
    try {
      server.waitFor();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testMACIsCalculated() {
    KVMessage msg = new KVMessage("hello", "world", KVMessage.StatusType.PUT).calculateMAC();
    assertNotNull(msg.getMAC());
  }

  @Test
  public void simpleEncryptionAndDecryptionWorks() throws EncryptionException {
    String source = "hello world!";
    PublicKey publicKey = ServerPropertyStore.getInstance().getClientPublicKey();
    PrivateKey privateKey = ClientPropertyStore.getInstance().getPrivateKey();
    String encryptedSource =
        Encryption.encryptString(source, privateKey, Encryption.EncryptionType.RSA);
    String decryptedSource =
        Encryption.decryptString(encryptedSource, publicKey, Encryption.EncryptionType.RSA);
    assertEquals(source, decryptedSource);
  }

  @Test
  public void testUnTamperedMessageCanBeVerified() {
    KVMessage msg = new KVMessage("hello", "world", KVMessage.StatusType.PUT).calculateMAC();
    try {
      assertTrue(Verifier.verifyKVMessageMAC(msg));
    } catch (EncryptionException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testKVMessageWithEmptyMACFails() {
    KVMessage msg = new KVMessage("hello", "world", KVMessage.StatusType.PUT);
    KVMessage response = sendRequestViaSocket(new ECSNode("localhost", 50001), msg);
    assertNotNull(response);
    assertSame(response.getStatus(), KVMessage.StatusType.AUTH_FAILED);
  }

  @Test
  public void testMessageSignedWithRandomPrivateKeyFails() throws EncryptionException {
    String json =
        "{\"key\":\"hello\",\"value\":\"world\",\"senderID\":\"client\",\"statusType\":\"PUT\",\"MAC\":\"%s\",\"timestamp\":\"1617849240846\",\"requestId\":\"5a16cdce-0d30-4509-8423-e4c6231a9ace\"}";
    KVMessage messageWithoutMAC = new Gson().fromJson(json, KVMessage.class);
    String hash = messageWithoutMAC.generateMessageHash();

    KeyGenerator kg = new KeyGenerator(1024);
    kg.createKeys();
    String MAC = Encryption.encryptString(hash, kg.getPrivateKey(), Encryption.EncryptionType.RSA);
    String msgJson = String.format(json, MAC);
    KVMessage forgedMessage = new Gson().fromJson(msgJson, KVMessage.class);

    KVMessage response = sendRequestViaSocket(new ECSNode("localhost", 50001), forgedMessage);
    assertNotNull(response);
    assertSame(KVMessage.StatusType.AUTH_FAILED, response.getStatus());
  }

  @Test
  public void testKVMessageReplayFails() throws IOException, ProtocolException {
    KVMessage msg = new KVMessage("hello", "world", KVMessage.StatusType.PUT);
    msg.calculateMAC();

    KVMessage response =
        (KVMessage) TestUtils.sendMessageToNode(new ECSNode("localhost", 50001), msg);
    assertSame(response.getStatus(), KVMessage.StatusType.PUT_SUCCESS);

    KVMessage response2 =
        (KVMessage) TestUtils.sendMessageToNode(new ECSNode("localhost", 50001), msg);
    assertSame(response2.getStatus(), KVMessage.StatusType.AUTH_FAILED);
  }

  @Test
  public void testEncryptionAndDecryptionWorksOnBytes() {
    String msg = "Hello World!";
    try {
      byte[] encryptedMsg;
      encryptedMsg =
          Encryption.encryptBytes(
              msg.getBytes(StandardCharsets.UTF_8),
              ClientPropertyStore.getInstance().getPrivateKey(),
              Encryption.EncryptionType.RSA);
      byte[] decryptedMsg =
          Encryption.decryptBytes(
              encryptedMsg,
              ServerPropertyStore.getInstance().getClientPublicKey(),
              Encryption.EncryptionType.RSA);
      String msg2 = new String(decryptedMsg);

      assertEquals(msg, msg2);

    } catch (EncryptionException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testEncryptionAndDecryptionWorksOnStrings() {
    String msg = "Hello World!";
    try {
      String encryptedMsg =
          Encryption.encryptString(
              msg,
              ClientPropertyStore.getInstance().getPrivateKey(),
              Encryption.EncryptionType.RSA);
      String msg2 =
          Encryption.decryptString(
              encryptedMsg,
              ServerPropertyStore.getInstance().getClientPublicKey(),
              Encryption.EncryptionType.RSA);
      assertEquals(msg, msg2);

    } catch (EncryptionException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testKVMessageFromServerDoesNotWork() {
    String json =
        "{\"key\":\"hello\",\"value\":\"world\",\"senderID\":\"localhost:50001\",\"statusType\":\"PUT\",\"MAC\":\"%s\",\"timestamp\":\"1617849240846\",\"requestId\":\"5a16cdce-0d30-4509-8423-e4c6231a9ace\"}";
    KVMessage messageFromServer = new Gson().fromJson(json, KVMessage.class).calculateMAC();
    KVMessage response = sendRequestViaSocket(new ECSNode("localhost", 50001), messageFromServer);
    assertNotNull(response);
    assertSame(KVMessage.StatusType.AUTH_FAILED, response.getStatus());
  }

  @Test
  public void testCannotModifyPropertyStore() {
    String s1 = ClientPropertyStore.getInstance().getSenderID();
    if (s1 == null) {
      s1 = "xyz";
      ClientPropertyStore.getInstance().setSenderID("xyz");
    }
    ClientPropertyStore.getInstance().setSenderID("hello");
    assertEquals(s1, ClientPropertyStore.getInstance().getSenderID());
  }

  private KVMessage sendRequestViaSocket(ECSNode server, KVMessage request) {
    try {
      TimeUnit.SECONDS.sleep(1);

      Socket serverSocket =
          new Socket(InetAddress.getByName(server.getNodeHost()), server.getNodePort());
      InputStream inputStream = serverSocket.getInputStream();
      OutputStream outputStream = serverSocket.getOutputStream();

      Protocol.sendMessage(outputStream, request);
      return (KVMessage) Protocol.receiveMessage(inputStream);

    } catch (InterruptedException | IOException | ProtocolException e) {
      fail("Error in get dataset");
      e.printStackTrace();
    }
    return null;
  }
}
