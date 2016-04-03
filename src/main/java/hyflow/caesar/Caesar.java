package hyflow.caesar;

import hyflow.caesar.messages.*;
import hyflow.caesar.network.*;
import hyflow.common.ProcessDescriptor;
import hyflow.common.Request;
import hyflow.common.ThreadDispatcher;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.BitSet;
import java.util.Queue;

public class Caesar {
    private final static Logger logger = LogManager.getLogger(Caesar.class);

    private final Proposer proposer;
    private final ThreadDispatcher dispatcher;
    private final UdpNetwork udpNetwork;
    private final Network network;
    private final ProcessDescriptor pd;

    private DecideCallback callback;


    public Caesar() throws IOException {
        this.pd = ProcessDescriptor.getInstance();

        this.dispatcher = new ThreadDispatcher("Caesar", pd.numThreads);

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
        ConflictDetector cDetector = new ConflictDetector(10000);

        this.proposer = new Proposer(tsGen, cDetector, network, dispatcher, this);
    }

    public void startCaesar(DecideCallback callback) {

        logger.warn("startCaesar");
        this.callback = callback;

        MessageHandler handler = new MessageHandlerImpl();
        Network.addMessageListener(MessageType.Alive, handler);
        Network.addMessageListener(MessageType.Propose, handler);
        Network.addMessageListener(MessageType.ProposeReply, handler);
        Network.addMessageListener(MessageType.Retry, handler);
        Network.addMessageListener(MessageType.RetryReply, handler);
        Network.addMessageListener(MessageType.Stable, handler);

        udpNetwork.start();
        network.start();
    }

    public void deliver(Request request, Queue<Runnable> deliverQ) {
        this.callback.deliver(request, deliverQ);
    }

    public void propose(final Request request) {
        dispatcher.execute(() -> proposer.propose(request));
    }

    public Network getNetwork() {
        return network;
    }

    public void onDelivery(Request request, Queue<Runnable> deliverQ) {
        proposer.onDelivery(request, deliverQ);
    }

    private class MessageHandlerImpl implements MessageHandler {
        public void onMessageReceived(Message msg, int sender) {
            logger.trace("Msg rcv: " + msg);
            MessageEvent event = new MessageEvent(msg, sender);
            dispatcher.submit(event);
        }

        public void onMessageSent(Message message, BitSet destinations) {
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
                switch (msg.getType()) {
                    case Propose:
                        proposer.onPropose((Propose) msg, sender);
                        break;

                    case ProposeReply:
                        proposer.onProposeReply((ProposeReply) msg, sender);
                        break;

                    case Retry:
                        proposer.onRetry((Retry) msg, sender);
                        break;

                    case RetryReply:
                        proposer.onRetryReply((RetryReply) msg, sender);
                        break;

                    case Stable:
                        logger.debug("Stable triggering");
                        proposer.onStable((Stable) msg, sender);
                        break;

                    case Alive:
                        logger.warn("Alive message received");
                        //TODO: Implement Handler
                        break;

                    default:
                        logger.warn("Unknown message type: " + msg);
                }
            } catch (Throwable t) {
                logger.log(Level.FATAL, "Unexpected exception", t);
                t.printStackTrace();
            }
        }

    }

}
