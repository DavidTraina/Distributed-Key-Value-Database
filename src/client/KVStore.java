package client;

import static shared.communication.messages.KVMessage.StatusType.GET;
import static shared.communication.messages.KVMessage.StatusType.GET_ERROR;
import static shared.communication.messages.KVMessage.StatusType.NOTIFY;
import static shared.communication.messages.KVMessage.StatusType.NOT_RESPONSIBLE;
import static shared.communication.messages.KVMessage.StatusType.PUT;
import static shared.communication.messages.KVMessage.StatusType.SUBSCRIBE;
import static shared.communication.messages.KVMessage.StatusType.UNSUBSCRIBE;

import app_kvClient.KVClient;
import ecs.ECSMetadata;
import ecs.ECSNode;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.spec.InvalidKeySpecException;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.log4j.Logger;
import org.jetbrains.annotations.NotNull;
import org.thavam.util.concurrent.BlockingHashMap;
import shared.communication.Protocol;
import shared.communication.ProtocolException;
import shared.communication.messages.ClientIdentificationMessage;
import shared.communication.messages.ClientServerMessage;
import shared.communication.messages.KVMessage;
import shared.communication.messages.Message;
import shared.communication.messages.MetadataUpdateMessage;
import shared.communication.security.KeyLoader;
import shared.communication.security.property_stores.ClientPropertyStore;

public class KVStore implements KVCommInterface {
  private static final Logger logger = Logger.getLogger(KVStore.class);
  private final AtomicReference<InetAddress> address = new AtomicReference<>();
  private final AtomicInteger port = new AtomicInteger();
  private final AtomicReference<Socket> clientSocket = new AtomicReference<>();
  private final AtomicReference<ECSMetadata> metadata = new AtomicReference<>();
  private final AtomicBoolean connected = new AtomicBoolean();
  private final BlockingHashMap<UUID, ClientServerMessage> replies = new BlockingHashMap<>();
  private final UUID clientId;

  /**
   * Initialize KVStore with address and port of KVServer
   *
   * @param address the address of the KVServer
   * @param port the port of the KVServer
   */
  public KVStore(InetAddress address, int port) {
    this.address.set(address);
    this.port.set(port);
    this.clientId = UUID.randomUUID();
    initializeClientPrivateKey();
    ClientPropertyStore.getInstance().setSenderID("client");
  }

  public static void initializeClientPrivateKey() {
    try {
      // Set Private Key For KV Store
      ClientPropertyStore.getInstance()
          .setPrivateKey(KeyLoader.getPrivateKey(ClientPrivateKey.base64EncodedPrivateKey));
    } catch (InvalidKeySpecException e) {
      logger.error("Client private key is invalid");
      e.printStackTrace();
    }
  }

  private void listen() {
    // poll metadata every 10 seconds
    ScheduledExecutorService scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
    ScheduledFuture<?> metadataPoller =
        scheduledExecutor.scheduleAtFixedRate(
            () -> {
              try {
                synchronized (clientSocket) {
                  Protocol.sendMessage(
                      clientSocket.get().getOutputStream(),
                      new MetadataUpdateMessage(null, new UUID(0, 0)));
                }
              } catch (IOException e) {
                logger.error("Error sending metadata request");
              }
            },
            0,
            10,
            TimeUnit.SECONDS);

    // listen for replies
    Socket listeningOn = null;
    while (connected.get()) {
      try {
        listeningOn = clientSocket.get();

        ClientServerMessage reply =
            (ClientServerMessage) Protocol.receiveMessage(listeningOn.getInputStream());

        // if a reply is a KVMessage, then it should be for this client
        assert (reply.getClass() != KVMessage.class
            || ((KVMessage) reply).getClientId().equals(clientId));
        logger.info("Received message: " + reply + "from: " + listeningOn);

        if (reply.getRequestId().equals(new UUID(0, 0))
            && reply.getClass() == MetadataUpdateMessage.class) {
          // Metadata was sent by server because metadataPoller requested it (so has 0 requestId)
          metadata.set(((MetadataUpdateMessage) reply).getMetadata());
          logger.info("Metadata has been updated with new metadata from server: " + metadata.get());
        } else if (reply.getClass() == KVMessage.class
            && ((KVMessage) reply).getStatus() == NOTIFY) {
          // Subscription notification
          // Should have 0 requestId since client did not request it
          assert (reply.getRequestId().equals(new UUID(0, 0)));
          KVClient.receiveSubscriptionNotification((KVMessage) reply);
        } else {
          assert (!reply.getRequestId().equals(new UUID(0, 0)));
          // Response to an explicit request made by the user, thus has a non-zero requestId
          ClientServerMessage prevMapping = replies.put(reply.getRequestId(), reply);
          // Should not be receiving multiple replies for same request
          assert (prevMapping == null);
          logger.info("Response to client request " + reply.getRequestId() + " received: " + reply);
        }
      } catch (IOException | ProtocolException | NullPointerException e) {
        if (!connected.get()) {
          // externally disconnected: gracefully let thread die.
          logger.info("Connection closed externally");
          break;
        }

        synchronized (clientSocket) {
          if (clientSocket.get() != listeningOn && connected.get()) {
            // Connection was reset: continue
            logger.info(
                "Client connected to a new node. Previously connected to "
                    + listeningOn
                    + ", now connected to "
                    + clientSocket.get());
            continue;
          }
        }

        // Attempt to reconnect
        logger.error(
            "Connection to " + clientSocket + " unexpectedly lost. Reconnecting to service.", e);
        final Socket sockToClose;
        synchronized (clientSocket) {
          // logically disconnect until new connection found
          connected.set(false);
          sockToClose = clientSocket.getAndSet(null);
        }
        closeSocket(sockToClose);
        restoreConnection();
        if (!connected.get()) {
          logger.fatal("Connection to service lost, all known nodes offline: " + metadata);
        }
      }
    }
    metadataPoller.cancel(true);
    scheduledExecutor.shutdownNow();
  }

  private boolean connectToNode(@NotNull ECSNode node) {
    logger.info("Attempting to connect to node: " + node);
    try {
      final InetAddress newAddr = InetAddress.getByName(node.getNodeHost());
      final int newPort = node.getNodePort();
      if (newAddr == address.get() && newPort == port.get() && connected.get()) {
        logger.info("Already connected to correct node");
        return true;
      }
      final Socket newConn = new Socket(newAddr, newPort);
      final Socket oldConn;
      synchronized (clientSocket) {
        oldConn = clientSocket.getAndSet(newConn);
        address.set(newAddr);
        port.set(newPort);
        try {
          sendRequest(new ClientIdentificationMessage(clientId));
        } catch (KVStoreException e) {
          connected.set(false);
          logger.error("Connection identification failed", e);
          closeSocket(newConn);
          closeSocket(oldConn);
          return false;
        }
        connected.set(true); // logically reconnect
        logger.info("New connection to node: " + node + " on socket " + clientSocket);
      }
      closeSocket(oldConn);
      return true;
    } catch (UnknownHostException e2) {
      logger.error("Unable to resolve hostname for node " + node.getNodeName(), e2);
    } catch (IOException e2) {
      logger.error("Failed to connect to node: " + node.getNodeName(), e2);
    }
    return false;
  }

  private void closeSocket(Socket sockToClose) {
    logger.info("Attempting to close socket " + sockToClose);
    if (sockToClose != null && !sockToClose.isClosed()) {
      try {
        sockToClose.close();
        logger.info("Connection closed successfully");
      } catch (IOException e) {
        logger.error("(non fatal) Could not close connection to " + sockToClose, e);
      }
    } else {
      logger.info("Connection was already closed");
    }
  }

  private void restoreConnection() {
    logger.info("Restoring connection...");
    ECSMetadata meta = metadata.get();
    if (meta == null) {
      logger.info("No metadata available, unable to restore connection.");
      return;
    }
    for (ECSNode node : meta.getNodeRing()) {
      if (connectToNode(node) && connected.get()) {
        logger.info("Connection restored to node: " + node);
        return;
      }
    }
  }

  @Override
  public void connect() throws KVStoreException {
    if (connected.get()) {
      throw new KVStoreException(
          "Already connected. Must disconnect before connecting to another node");
    }

    try {
      Socket newConn = new Socket(address.get(), port.get());
      synchronized (clientSocket) {
        clientSocket.set(newConn);
        connected.set(true);
      }
    } catch (IOException | NullPointerException e) {
      logger.error("Could not open connection to address " + address + " on port " + port, e);
      throw new KVStoreException("Error on connect: " + e.getMessage());
    }

    new Thread(this::listen).start();
  }

  @Override
  public void disconnect() throws KVStoreException {
    logger.info("Tearing down the connection ...");
    Socket sockToClose;
    synchronized (clientSocket) {
      connected.set(false); // logically disconnect first
      sockToClose = clientSocket.getAndSet(null); // lose reference to socket
    }
    closeSocket(sockToClose);
  }

  @Override
  public KVMessage put(String key, String value) throws KVStoreException {
    KVMessage request = new KVMessage(key, value, clientId, PUT).calculateMAC();
    return (KVMessage) sendRequestAndTakeReply(request);
  }

  @Override
  public KVMessage get(String key) throws KVStoreException {
    try {
      KVMessage messageToSend = new KVMessage(key, null, clientId, GET).calculateMAC();
      return sendGetRequestAndEnsureMajority(messageToSend);
    } catch (ByzantineException e) {
      logger.error("Byzantine error for key: " + key + " with message " + e.getMessage());
      throw new KVStoreException(
          "Majority not reached for key: "
              + key
              + " there might be a security breach on the servers.");
    }
  }

  @Override
  public KVMessage subscribe(String key) throws KVStoreException {
    KVMessage request = new KVMessage(key, null, clientId, SUBSCRIBE).calculateMAC();
    return (KVMessage) sendRequestAndTakeReply(request);
  }

  @Override
  public KVMessage unsubscribe(String key) throws KVStoreException {
    KVMessage request = new KVMessage(key, null, clientId, UNSUBSCRIBE).calculateMAC();
    return (KVMessage) sendRequestAndTakeReply(request);
  }

  private void sendRequest(Message request) throws KVStoreException {
    logger.info("Sending request: " + request);
    if (request.getClass() == KVMessage.class) {
      connectToCorrectNode(((KVMessage) request).getKey());
    }
    sendRequestWithRetry(request);
  }

  private void sendRequestToNode(KVMessage request, ECSNode nodeToSend) throws KVStoreException {
    logger.info(
        "Sending request with ID "
            + request.getRequestId()
            + " : "
            + request
            + " to node: "
            + nodeToSend.getNodeName());
    connectToNode(nodeToSend);
    sendRequestWithRetry(request);
  }

  private void sendRequestWithRetry(Message request) throws KVStoreException {
    try {
      synchronized (clientSocket) {
        Protocol.sendMessage(clientSocket.get().getOutputStream(), request);
      }
      logger.info("Sent request: " + request);
    } catch (IOException | NullPointerException e) {
      restoreConnection();
      if (connected.get()) {
        sendRequest(request);
      } else {
        logger.fatal("Connection to service lost, all known nodes offline: " + metadata);
        throw new KVStoreException(
            "Connection to service lost, all known nodes offline: " + metadata);
      }
    }
  }

  private ClientServerMessage sendRequestAndTakeReply(ClientServerMessage request)
      throws KVStoreException {
    sendRequest(request);
    return takeReply(request);
  }

  private KVMessage sendGetRequestAndEnsureMajority(KVMessage request)
      throws KVStoreException, ByzantineException {
    KVMessage replyOriginal = (KVMessage) sendRequestAndTakeReply(request);

    // In case metadata has not been initialized assume majority
    if (metadata.get() == null || replyOriginal.getStatus() == GET_ERROR) {
      return replyOriginal;
    }

    ECSNode correctNode = metadata.get().getNodeBasedOnKey(request.getKey());

    ECSNode[] replicas = metadata.get().getReplicasBasedOnName(correctNode.getNodeName());

    if (replicas == null || replicas.length == 0) {
      return replyOriginal;
    } else {
      boolean majority = false;
      for (ECSNode replica : replicas) {
        sendRequestToNode(request, replica);
        KVMessage replyReplica = (KVMessage) takeReply(request);
        majority =
            majority
                || (replyOriginal.getValue() == null && replyReplica.getValue() == null)
                || (replyOriginal.getValue() != null
                    && replyOriginal.getValue().equals(replyReplica.getValue()));
      }
      if (!majority) {
        throw new ByzantineException("Majority not reached for key: " + request.getKey());
      } else {
        return replyOriginal;
      }
    }
  }

  private ClientServerMessage takeReply(ClientServerMessage request) throws KVStoreException {
    ClientServerMessage reply;
    while (true) {
      try {
        reply = replies.take(request.getRequestId(), 5, TimeUnit.SECONDS);
        break;
      } catch (InterruptedException e) {
        logger.debug("take interrupted. Retrying.");
      }
    }

    if (reply == null) {
      logger.error("Failed to receive response for request " + request.getRequestId());
      throw new KVStoreException("No reply from server");
    }

    if (reply.getClass() == KVMessage.class && ((KVMessage) reply).getStatus() == NOT_RESPONSIBLE) {
      MetadataUpdateMessage metaReply =
          (MetadataUpdateMessage) sendRequestAndTakeReply(new MetadataUpdateMessage());
      metadata.set(metaReply.getMetadata());
      return sendRequestAndTakeReply(request);
    }

    return reply;
  }

  private boolean connectToCorrectNode(final String key) {
    ECSMetadata ecsMeta = metadata.get();
    if (ecsMeta == null) {
      logger.error("No metadata available.");
      return false;
    }

    ECSNode node = ecsMeta.getNodeBasedOnKey(key);
    if (node == null) {
      logger.error("Unable to find correct node.");
      return false;
    }

    return connectToNode(node) && connected.get();
  }
}
