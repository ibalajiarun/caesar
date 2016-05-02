package hyflow.caesar;

import hyflow.caesar.messages.*;
import hyflow.caesar.network.*;
import hyflow.common.Pair;
import hyflow.common.ProcessDescriptor;
import hyflow.common.Request;
import hyflow.common.ThreadDispatcher;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;

public final class Caesar implements FailureDetector.FailureDetectorListener {

    private final static Logger logger = LogManager.getLogger(Caesar.class);

    private final TimestampGenerator tsGen;
    private final ThreadDispatcher dispatcher;
    private final UdpNetwork udpNetwork;
    private final Network network;
    private final ProcessDescriptor pd;
    private final int totalObjects;
    private final FailureDetector failureDetector;
    private Proposer proposer;
    private ConflictDetector cDetector;
    private DecideCallback callback;

    private Map<String, Pair<Integer, Integer>> barrierMap = new HashMap<String, Pair<Integer, Integer>>();

    public Caesar(int totalObjects) throws IOException {
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

        failureDetector = new FailureDetector(this, udpNetwork);

        this.totalObjects = totalObjects;

        this.tsGen = new TimestampGenerator(pd.localId, pd.numReplicas);
        this.cDetector = new ConflictDetector(totalObjects);

        this.proposer = new Proposer(tsGen, cDetector, network, dispatcher, this);
    }

    public void startCaesar(DecideCallback callback) {

        logger.warn("startCaesar");
        this.callback = callback;

        MessageHandler handler = new MessageHandlerImpl();

        Network.addMessageListener(MessageType.FastPropose, handler);
        Network.addMessageListener(MessageType.FastProposeReply, handler);

        Network.addMessageListener(MessageType.SlowPropose, handler);
        Network.addMessageListener(MessageType.SlowProposeReply, handler);

        Network.addMessageListener(MessageType.Retry, handler);
        Network.addMessageListener(MessageType.RetryReply, handler);

        Network.addMessageListener(MessageType.Stable, handler);

        Network.addMessageListener(MessageType.Recovery, handler);
        Network.addMessageListener(MessageType.RecoveryReply, handler);

        Network.addMessageListener(MessageType.Barrier, handler);

        udpNetwork.start();
        network.start();


        failureDetector.start();
    }

    public void deliver(Request request, Queue<Runnable> deliverQ) {
        this.callback.deliver(request, deliverQ);
    }

    public void propose(final Request request) {
        dispatcher.execute(() -> proposer.fastPropose(request));
    }

    public Network getNetwork() {
        return network;
    }

    public void onDelivery(Request request, Queue<Runnable> postDelQ) {
        proposer.onDelivery(request, postDelQ);
    }

    public void refresh() {
        cDetector = new ConflictDetector(totalObjects);
        this.proposer = new Proposer(tsGen, cDetector, network, dispatcher, this);
    }

    private void processBarrierPackage(BarrierPackage barrierPackage) {
        synchronized (barrierMap) {
            Pair<Integer, Integer> pair = barrierMap.get(barrierPackage.barrierName);
            if (pair == null) {
                pair = new Pair<Integer, Integer>(barrierPackage.n, 0);
                barrierMap.put(barrierPackage.barrierName, pair);
            }

            Integer alreadyIn = pair.second + 1;
            pair.second = alreadyIn;
            if (alreadyIn >= barrierPackage.n - 1) {
                synchronized (pair) {
                    pair.notifyAll();
                }
                if (alreadyIn == barrierPackage.n)
                    barrierMap.remove(barrierPackage.barrierName);
            }
        }
    }

    public void enterBarrier(String name, int n) {
        if (n <= 1)
            return;

        BarrierPackage barrierPackage = new BarrierPackage(name, n);
        Pair<Integer, Integer> pair;
        synchronized (barrierMap) {
            pair = barrierMap.get(barrierPackage.barrierName);
            if (pair == null) {
                pair = new Pair<Integer, Integer>(barrierPackage.n, 0);
                barrierMap.put(barrierPackage.barrierName, pair);
            }
        }

        synchronized (pair) {
            try {
                network.sendToAll(barrierPackage);
                while (pair.second != pair.first) {
                    pair.wait();
                }
                if (pair.first != pair.second) {
                    pair.wait(100);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        synchronized (barrierMap) {
            barrierMap.remove(barrierPackage.barrierName);
        }
    }

    @Override
    public void suspect(int nodeId) {
        dispatcher.submit(() -> proposer.startRecovery(nodeId));
    }

    private final class MessageHandlerImpl implements MessageHandler {
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
                    case FastPropose:
                        proposer.onFastPropose((FastPropose) msg, sender);
                        break;

                    case FastProposeReply:
                        proposer.onFastProposeReply((FastProposeReply) msg, sender);
                        break;

                    case SlowPropose:
                        proposer.onSlowPropose((SlowPropose) msg, sender);
                        break;

                    case SlowProposeReply:
                        proposer.onSlowProposeReply((SlowProposeReply) msg, sender);
                        break;

                    case Retry:
                        proposer.onRetry((Retry) msg, sender);
                        break;

                    case RetryReply:
                        proposer.onRetryReply((RetryReply) msg, sender);
                        break;

                    case Stable:
                        proposer.onStable((Stable) msg, sender);
                        break;

                    case Recovery:
                        proposer.onRecovery((Recovery) msg, sender);
                        break;

                    case RecoveryReply:
                        proposer.onRecoveryReply((RecoveryReply) msg, sender);
                        break;

                    case Alive:
                        logger.trace("Alive message received");
                        //TODO: Implement Handler
                        break;

                    case Barrier:
                        processBarrierPackage((BarrierPackage) msg);
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
