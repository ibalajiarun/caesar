package hyflow.caesar;

import hyflow.caesar.messages.Alive;
import hyflow.caesar.messages.Message;
import hyflow.caesar.messages.MessageType;
import hyflow.caesar.network.MessageHandler;
import hyflow.caesar.network.Network;
import hyflow.common.ProcessDescriptor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.BitSet;

/**
 * Represents failure detector thread. If the current process is the leader,
 * then this class is responsible for sending <code>ALIVE</code> message every
 * amount of time. Otherwise is responsible for suspecting the leader. If there
 * is no message received from leader, then the leader is suspected to crash,
 * and <code>Paxos</code> is notified about this event.
 */
final public class FailureDetector implements Runnable {
    private final static Logger logger = LogManager.getLogger(FailureDetector.class);
    /**
     * How long to wait until suspecting the leader. In milliseconds
     */
    private final int suspectTimeout;
    /**
     * How long the leader waits until sending heartbeats. In milliseconds
     */
    private final int sendTimeout;
    private final Network network;
    private final MessageHandler innerListener;
    private final Thread thread;
    private final ProcessDescriptor pd;
    private final FailureDetectorListener fdListener;
    private volatile BitSet active;
    private volatile long[] lastHeartbeatRcvdTS;
    private volatile long lastHeartbeatSentTS;

    /**
     * Initializes new instance of <code>FailureDetector</code>.
     *
     * @param network - used to send and receive messages
     */
    public FailureDetector(FailureDetectorListener fdListener, Network network) {
        this.fdListener = fdListener;
        this.network = network;
        this.pd = ProcessDescriptor.getInstance();
        this.suspectTimeout = pd.fdSuspectTimeout;
        this.sendTimeout = pd.fdSendTimeout;
        this.thread = new Thread(this, "FailureDetector");
        this.innerListener = new InnerMessageHandler();
        this.lastHeartbeatRcvdTS = new long[pd.numReplicas];
        this.active = new BitSet(pd.numReplicas);
    }

    static long getTime() {
        return System.nanoTime() / 1000000;
    }

    /**
     * Starts failure detector.
     */
    public void start() {
        synchronized (this) {
            thread.start();
        }

        Network.addMessageListener(MessageType.ANY, innerListener);
        Network.addMessageListener(MessageType.SENT, innerListener);
    }

    /**
     * Stops failure detector.
     */
    public void stop() {
        Network.removeMessageListener(MessageType.ANY, innerListener);
        Network.removeMessageListener(MessageType.SENT, innerListener);
    }

    public void run() {
        logger.info("Starting failure detector");
        try {
            // Warning for maintainers: Deadlock danger!!
            // The code below calls several methods in other classes while holding the this lock.
            // If the methods called acquire locks and then try to call into this failure detector,
            // there is the danger of deadlock. Therefore, always ensure that the methods called
            // below do not themselves obtain locks.
            synchronized (this) {
                long now = getTime();

                if (!pd.isLocalProcessLeader()) {

                    while (true) {

                        Alive alive = new Alive();
                        network.sendMessage(alive, pd.recoveryLeader);
                        lastHeartbeatSentTS = now;
                        long nextSend = lastHeartbeatSentTS + sendTimeout;

                        while (now < nextSend) {
                            if (logger.isTraceEnabled()) {
                                logger.trace("Sending next Alive in " + (nextSend - now) + "ms");
                            }
                            wait(nextSend - now);

                            now = getTime();
                            nextSend = lastHeartbeatSentTS + sendTimeout;
                        }

                    }

                } else {

                    active.set(0, active.size() - 1);

                    while (true) {
                        long suspectTime = now + suspectTimeout;

                        while (now < suspectTime) {
                            wait(suspectTime - now);
                            now = getTime();
                        }

                        for (int id = 0; id < pd.numReplicas; id++) {
                            if (id != pd.localId) {
                                if (now > lastHeartbeatRcvdTS[id] + suspectTimeout && active.get(id)) {
                                    active.set(id, false);
                                    fdListener.suspect(id);
                                }
                            }
                        }
                    }
                }
            }
        } catch (InterruptedException ex) {
            logger.warn("Thread dying: " + ex.getMessage());
        }
    }

    public interface FailureDetectorListener {

        public void suspect(int nodeId);

    }

    /**
     * Intersects any message sent or received, used to reset the timeouts for
     * sending and receiving ALIVE messages.
     * <p>
     * These methods are called by the Network thread.
     *
     * @author Nuno Santos (LSR)
     */
    final class InnerMessageHandler implements MessageHandler {

        public void onMessageReceived(Message message, int sender) {
            if (pd.isLocalProcessLeader()) {
                lastHeartbeatRcvdTS[sender] = getTime();
            }
        }

        public void onMessageSent(Message message, BitSet destinations) {
            if (message.getType() == MessageType.Alive) {
                return;
            }

            if (!destinations.get(pd.recoveryLeader)) {
                return;
            }

            if (!pd.isLocalProcessLeader()) {
                lastHeartbeatSentTS = getTime();
            }
        }
    }
}
