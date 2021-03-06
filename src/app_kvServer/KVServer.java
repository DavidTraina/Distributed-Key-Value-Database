package app_kvServer;

import app_kvECS.ZKManager;
import app_kvServer.data.SynchronizedKVManager;
import client.ByzantineException;
import ecs.ECSMetadata;
import ecs.ECSNode;
import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.spec.InvalidKeySpecException;
import java.util.ArrayList;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import shared.communication.messages.ECSMessage;
import shared.communication.messages.KVMessage;
import shared.communication.messages.Message;
import shared.communication.messages.MessageException;
import shared.communication.security.KeyLoader;
import shared.communication.security.keys.ClientPublicKey;
import shared.communication.security.keys.ECSPublicKey;
import shared.communication.security.property_stores.ServerPropertyStore;

public class KVServer implements Runnable {

  private static final Logger logger = Logger.getLogger(KVServer.class);

  private final int port;
  private final AtomicBoolean isRunning = new AtomicBoolean();
  private final AtomicBoolean serverAcceptingClients = new AtomicBoolean(true);
  private final ZKManager zkManager;
  private volatile ServerSocket serverSocket;
  private final LinkedBlockingQueue<KVMessage> replicationQueue = new LinkedBlockingQueue<>();
  private final String nodeName;
  private ReplicationService replicationService;
  private final Set<String> ecsIDs = ConcurrentHashMap.newKeySet();

  // Constructor used when running standalone server
  public KVServer(final int port) throws ByzantineException {
    try {
      new LogSetup("logs/server_" + port + ".log", Level.INFO, false);
    } catch (IOException e) {
      System.out.println("Error! Unable to initialize logger!");
      e.printStackTrace();
      System.exit(1);
    }

    this.port = port;
    zkManager = null;
    replicationService = null;

    // Initialize ECSMetadata for lone node i.e server responsible for all keys
    ECSNode loneNode = new ECSNode("localhost", port);
    ServerPropertyStore.getInstance().setSenderID(loneNode.getNodeName());
    loneNode.setLowerRange(loneNode.getNodeHash());
    ArrayList<ECSNode> allNodes = new ArrayList<>();
    allNodes.add(loneNode);
    ECSMetadata.initialize(allNodes);
    this.nodeName = loneNode.getNodeName();
    try {
      ServerPropertyStore.getInstance()
          .setClientPublicKey(KeyLoader.getPublicKey(ClientPublicKey.base64EncodedPublicKey));
      ServerPropertyStore.getInstance()
          .setECSPublicKey(KeyLoader.getPublicKey(ECSPublicKey.base64EncodedPublicKey));
    } catch (InvalidKeySpecException e) {
      logger.error("Some public key is invalid");
      throw new ByzantineException(e.getLocalizedMessage());
    }
  }

  // Constructor used by ECS i.e running a cluster of servers
  public KVServer(final int port, final String zkIP, final int zkPort, final String name)
      throws ByzantineException {
    try {
      new LogSetup("logs/server_" + port + ".log", Level.DEBUG, false);
    } catch (IOException e) {
      System.out.println("Error! Unable to initialize logger!");
      e.printStackTrace();
      System.exit(1);
    }
    ServerPropertyStore.getInstance().setSenderID(name);
    this.nodeName = name;
    this.port = port;
    this.serverAcceptingClients.set(false);
    ECSMetadata.initialize(new ArrayList<>());
    zkManager = new ZKManager(zkIP, zkPort);

    logger.info("Setting up zookeeper");
    // Get current metadata and setup a watch for later updates
    zkManager.getZNodeData(
        "/metadata",
        new Watcher() {
          @Override
          public void process(WatchedEvent event) {
            if (event.getType() == Event.EventType.NodeDataChanged
                && event.getPath().equals("/metadata")) {
              logger.info("Updating metadata -> metadata has been changed");
              byte[] updatedMetadata = zkManager.getZNodeData("/metadata", this);
              updateECSMetadata(updatedMetadata);
              logger.info("Metadata updated successfully");
            }
          }
        });

    // Create ephemeral znode to serve as a heartbeat
    zkManager.createEphemeral("/nodes/" + name, "I am alive".getBytes(StandardCharsets.UTF_8));
    logger.info("Created Ephemeral Node");

    try {
      ServerPropertyStore.getInstance()
          .setClientPublicKey(KeyLoader.getPublicKey(ClientPublicKey.base64EncodedPublicKey));
      ServerPropertyStore.getInstance()
          .setECSPublicKey(KeyLoader.getPublicKey(ECSPublicKey.base64EncodedPublicKey));
    } catch (InvalidKeySpecException e) {
      logger.error("Some public key is invalid");
      throw new ByzantineException(e.getLocalizedMessage());
    }
    logger.info("Set up public keys");

    logger.info("Done initializing KV Server");
  }

  private synchronized void updateECSMetadata(byte[] newMetadataBytes) {
    ECSMessage message;
    try {
      logger.info("Updating metadata");
      message = (ECSMessage) Message.deserialize(newMetadataBytes);
      ArrayList<ECSNode> newMetadata = message.getMetadata().getNodeRing();
      ArrayList<ECSNode> oldMetadata = ECSMetadata.getInstance().getNodeRing();

      // Start the replication service
      logger.info("Starting replication service");
      if (this.replicationService == null) {
        this.replicationService = new ReplicationService(replicationQueue, this.nodeName);
        new Thread(this.replicationService).start();
      }

      ECSMetadata.getInstance().update(message.getMetadata());
      SynchronizedKVManager.getInstance().clearCache();

      logger.info(ECSMetadata.getInstance().toString());
      if (this.replicationService != null) {
        logger.info("Asking replication service to handle metadata change");
        this.replicationService.handleMetadataChange(message, oldMetadata, newMetadata);
        logger.info("Metadata change handled by replication service");
      } else {
        logger.info("Replication service NOT initialized, skipping replication update operation");
      }
    } catch (MessageException e) {
      logger.error("Error deserialize metadata");
    }
  }

  @Override
  public void run() {
    initializeServerSocket();

    while (isRunning.get()) {
      try {
        final Socket clientSocket = serverSocket.accept();
        new Thread(
                new KVServerConnection(
                    clientSocket, serverAcceptingClients, this.replicationQueue, ecsIDs),
                "Conn Thread: " + clientSocket)
            .start();
        logger.info("New connection to " + clientSocket + " accepted.");
      } catch (IOException e) {
        if (!isRunning.get()) {
          logger.debug("Failed to accept client connection because server was stopped.\n", e);
        } else {
          logger.error("Unknown error accepting client connection. Continuing accept loop. \n", e);
        }
      }
    }
    logger.info("Server stopped");
  }

  public void stop() {
    logger.info("Stopping KVServer on " + serverSocket);
    if (serverSocket == null) {
      logger.error("Server is uninitialized.");
      throw new AssertionError("Server is uninitialized.");
    }
    isRunning.set(false);
    if (!serverSocket.isClosed()) {
      try {
        serverSocket.close();
      } catch (IOException e) {
        logger.error("Error! Unable to close " + serverSocket, e);
      }
    }
  }

  private void initializeServerSocket() {
    logger.info("Initializing server ...");
    if (serverSocket != null) {
      logger.error("Server on " + serverSocket + " is already initialized.");
      throw new AssertionError("Server already initialized");
    }
    try {
      serverSocket = new ServerSocket(port, 1000);
    } catch (BindException e) {
      logger.error("Error opening server socket: Port " + port + " is already bound!", e);
      return;
    } catch (IOException e) {
      logger.error("Error opening server socket: ", e);
      return;
    }
    String newThreadName = "Server Thread: " + serverSocket;
    logger.info("Renaming current thread to '" + newThreadName + "'");
    Thread.currentThread().setName(newThreadName);
    logger.info("Server listening on " + serverSocket);
    isRunning.set(true);
  }
}
