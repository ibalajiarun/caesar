package hyflow.caesar;

import java.io.IOException;
import java.util.BitSet;

import hyflow.caesar.network.GenericNetwork;
import hyflow.caesar.network.MessageHandler;
import hyflow.caesar.network.UdpNetwork;
import hyflow.common.ProcessDescriptor;
import hyflow.common.Request;
import hyflow.common.SingleThreadDispatcher;
import hyflow.caesar.messages.Message;
import hyflow.caesar.messages.MessageType;
import hyflow.caesar.network.Network;
import hyflow.caesar.network.TcpNetwork;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Implements state machine replication. It keeps a replicated log internally
 * and informs the listener of decisions using callbacks. This implementation is
 * monolithic, in the sense that leader election/view change are integrated on
 * the paxos protocol.
 *
 * <p>
 * The first consensus instance is 0. Decisions might not be reached in sequence
 * number order.
 * </p>
 */
public class Caesar {
    private final Proposer proposer;

    /**
     * Threading model - This class uses an event-driven threading model. It
     * starts a Dispatcher thread that is responsible for executing the
     * replication protocol and has exclusive access to the internal data
     * structures. The Dispatcher receives work using the pendingEvents queue.
     */
    /**
     * The Dispatcher thread executes the replication protocol. It receives and
     * executes events placed on the pendingEvents queue: messages from other
     * processes or proposals from the local process.
     *
     * Only this thread is allowed to access the state of the replication
     * protocol. Therefore, there is no need for synchronization when accessing
     * this state. The synchronization is handled by the
     * <code>pendingEvents</code> queue.
     */

//    private final Dispatcher dispatcher;
    private final SingleThreadDispatcher dispatcher;
    // udpNetwork is used by the failure detector, so it is always created
    private final UdpNetwork udpNetwork;
    // Can be a udp, tcp or generic network. Only a single udpnetwork is
    // created,
    // so the object held by udpNetwork might be reusued here, directly or via
    // GenericNetwork
    private final Network network;

    private final ProcessDescriptor pd;

    /**
     * Initializes new instance of {@link Caesar}.
     *
     *
     * @throws IOException if an I/O error occurs
     */
    public Caesar() throws IOException {
        this.pd = ProcessDescriptor.getInstance();

        // Handles the replication protocol and writes messages to the network
//        dispatcher = new DispatcherImpl("Protocol");
        this.dispatcher = new SingleThreadDispatcher("Protocol");

        // UDPNetwork is always needed because of the failure detector
        this.udpNetwork = new UdpNetwork();
        if (pd.network.equals("TCP")) {
            network = new TcpNetwork();
        } else if (pd.network.equals("UDP")) {
            network = udpNetwork;
        } else if (pd.network.equals("Generic")) {
            TcpNetwork tcp = new TcpNetwork();
            network = new GenericNetwork(tcp, udpNetwork);
        } else {
            throw new IllegalArgumentException("Unknown network type: " + pd.network +
                    ". Check paxos.properties configuration.");
        }
        logger.info("Network: " + network.getClass().getCanonicalName());


        TimestampGenerator tsGen = new TimestampGenerator(pd.localId, pd.numReplicas);
        ConflictDetector cDetector = new ConflictDetector(1000);

        this.proposer = new Proposer(tsGen, cDetector, network);

        // Crash tests: Simulate crashes. The constructor registers
        // a periodic task on the Protocol dispatcher
//        LeaderPromoter promoter = new LeaderPromoter(this);
    }


    /**
     * Joins this process to the paxos protocol. The catch-up and failure
     * detector mechanisms are started and message handlers are registered.
     */
    public void startCaesar() {
//        assert decideCallback != null : "Cannot start with null DecideCallback";

        logger.warn("startPaxos");
        MessageHandler handler = new MessageHandlerImpl();
        Network.addMessageListener(MessageType.Alive, handler);
        Network.addMessageListener(MessageType.Propose, handler);
        Network.addMessageListener(MessageType.ProposeReply, handler);
        Network.addMessageListener(MessageType.Retry, handler);
        Network.addMessageListener(MessageType.RetryReply, handler);
        Network.addMessageListener(MessageType.Stable, handler);

        // Starts the threads on the child modules. Should be done after
        // all the dependencies are established, ie. listeners registered.
        udpNetwork.start();
        network.start();
    }

    /**
     * Gets the dispatcher used by paxos to avoid concurrency in handling
     * events.
     *
     * @return current dispatcher object
     */
    public SingleThreadDispatcher getDispatcher() {
        return dispatcher;
    }

    /**
     * Changes state of specified consensus instance to <code>DECIDED</code>.
     *
     */
    public void decide() {

    }

    public void propose(Request request) {
    }

    // *****************
    // Auxiliary classes
    // *****************
    /**
     * Receives messages from other processes and stores them on the
     * pendingEvents queue for processing by the Dispatcher thread.
     */
    private class MessageHandlerImpl implements MessageHandler {
        public void onMessageReceived(Message msg, int sender) {
            logger.trace("Msg rcv: " + msg);
            MessageEvent event = new MessageEvent(msg, sender);
            dispatcher.submit(event);
        }

        public void onMessageSent(Message message, BitSet destinations) {
            // Empty
        }
    }

    private final class MessageEvent implements Runnable {
        private final Message msg;
        private final int sender;

        public MessageEvent(Message msg, int sender) {
            this.msg = msg;
            this.sender = sender;
        }

        public void run() {
            try {
                // The monolithic implementation of Paxos does not need Nack
                // messages because the Alive messages from the failure detector
                // are handled by the Paxos algorithm, so it can advance view
                // when it receives Alive messages. But in the modular
                // implementation, the Paxos algorithm does not use Alive
                // messages,
                // so if a process p is on a lower view and the system is idle,
                // p will remain in the lower view until there is another
                // request
                // to be ordered. The Nacks are required to force the process to
                // advance

                // Invariant for all message handlers: msg.view >= view
                switch (msg.getType()) {
                    case Propose:
                        break;

                    case ProposeReply:
                        break;

                    case Retry:
                        break;

                    case RetryReply:
                        break;

                    case Stable:
                        break;

                    case Alive:
                        break;

                    default:
                        logger.warn("Unknown message type: " + msg);
                }
            } catch (Throwable t) {
                logger.log(Level.FATAL, "Unexpected exception", t);
                t.printStackTrace();
                System.exit(1);
            }
        }

    }

    public Network getNetwork() {
        return network;
    }

    public Proposer getProposer() {
        return proposer;
    }


    private final static Logger logger = LogManager.getLogger(Caesar.class.getCanonicalName());

}
