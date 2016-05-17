package hyflow.caesar.network;

import hyflow.caesar.messages.Message;
import hyflow.caesar.messages.MessageFactory;
import hyflow.caesar.messages.MessageType;
import hyflow.common.ProcessDescriptor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * This class provides methods to communicate with other processes (replicas).
 * It allows to send the message to one or many replicas, and provides listeners
 * called every time new message is received or sent.
 *
 */
public abstract class Network {

    /**
     * For each message type, keeps a list of it's listeners.
     * <p>
     * The list is shared between networks
     */
    protected static final Map<MessageType, CopyOnWriteArrayList<MessageHandler>> msgListeners;
    private final static Logger logger = LogManager.getLogger(Network.class);

    static {
        msgListeners = Collections.synchronizedMap(
                new EnumMap<MessageType, CopyOnWriteArrayList<MessageHandler>>(MessageType.class));
        for (MessageType ms : MessageType.values()) {
            msgListeners.put(ms, new CopyOnWriteArrayList<MessageHandler>());
        }
    }

    // // // Public interface - send, send to all and add / remove listeners //
    // // //
    protected final int localId;
    protected final int N;
    protected final ProcessDescriptor p;
    protected final BitSet OTHERS;
    protected final BitSet ALL;

    public Network() {
        this.p = ProcessDescriptor.getInstance();
        this.localId = p.localId;
        this.N = p.numReplicas;
        this.OTHERS = new BitSet(N);
        OTHERS.set(0, N, true);
        OTHERS.clear(localId);

        this.ALL = new BitSet(N);
        ALL.set(0, N, true);
    }

    /**
     * Adds a new message listener for a certain type of message or all messages
     * ( see {@link MessageType}). The listener cannot be added twice for the
     * same message - this causes a {@link RuntimeException}.
     */
    final public static void addMessageListener(MessageType mType, MessageHandler handler) {
        CopyOnWriteArrayList<MessageHandler> handlers = msgListeners.get(mType);
        boolean wasAdded = handlers.addIfAbsent(handler);
        if (!wasAdded) {
            throw new RuntimeException("Handler already registered");
        }
    }

    /**
     * Removes a previously registered listener. Throws {@link RuntimeException}
     * if the listener is not on list.
     */
    final public static void removeMessageListener(MessageType mType, MessageHandler handler) {
        CopyOnWriteArrayList<MessageHandler> handlers = msgListeners.get(mType);
        boolean wasPresent = handlers.remove(handler);
        if (!wasPresent) {
            throw new RuntimeException("Handler not registered");
        }
    }

    public static void removeAllMessageListeners() {
        msgListeners.clear();
        for (MessageType ms : MessageType.values()) {
            msgListeners.put(ms, new CopyOnWriteArrayList<MessageHandler>());
        }
    }

    public abstract boolean send(byte[] message, int destination);

    /**
     * Sends the message to process with specified id.
     *
     * @param message the message to send
     * @param destinations bit set with marked replica id's to send message to
     */
    public abstract void sendMessage(Message message, BitSet destinations);

    /**
     * Sends the message to process with specified id.
     *
     * @param message the message to send
     * @param destination the id of replica to send message to
     */
    public void sendMessage(Message message, int destination) {
        byte[] bytes = message.toByteArray();
        if (destination == localId) {
            try {
                fireReceiveMessage(MessageFactory.create(new DataInputStream(new ByteArrayInputStream(bytes))), p.localId);
            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
            }
        } else {
            send(bytes, destination);
        }
    }

    // // // Protected part - for implementing the subclasses // // //

    /**
     * Sends the message to all processes.
     *
     * @param message the message to send
     */
    public void sendToAll(Message message) {
        sendMessage(message, ALL);
    }

    public void sendToOthers(Message message) {
        sendMessage(message, OTHERS);
    }

    public abstract void start();

    /**
     * Notifies all active network listeners that new message was received.
     */
    protected final void fireReceiveMessage(Message message, int sender) {
        assert message.getType() != MessageType.SENT && message.getType() != MessageType.ANY;
        boolean handled = broadcastToListeners(message.getType(), message, sender);
        handled |= broadcastToListeners(MessageType.ANY, message, sender);
        if (!handled) {
            logger.warn("Unhandled message: " + message);
        }
    }

    /**
     * Notifies all active network listeners that message was sent.
     */
    protected final void fireSentMessage(Message msg, BitSet dest) {
        List<MessageHandler> handlers = msgListeners.get(MessageType.SENT);
        for (MessageHandler listener : handlers) {
            listener.onMessageSent(msg, dest);
        }
    }

    /**
     * Informs all listeners waiting for the message type about the message.
     * Parameter type is needed in order to support MessageType.ANY value.
     * Returns if there was at least one listener.
     */
    private final boolean broadcastToListeners(MessageType type, Message msg, int sender) {
        List<MessageHandler> handlers = msgListeners.get(type);
        boolean handled = false;
        for (MessageHandler listener : handlers) {
            listener.onMessageReceived(msg, sender);
            handled = true;
        }
        return handled;
    }
}
