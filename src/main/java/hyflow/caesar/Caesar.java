package hyflow.caesar;

import hyflow.caesar.messages.*;
import hyflow.caesar.network.MessageHandler;
import hyflow.caesar.network.Network;
import hyflow.caesar.network.TcpNetwork;
import hyflow.caesar.network.UdpNetwork;
import hyflow.common.*;
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

    private final ScheduledThreadDispatcher cReqDispatcher;
    private final ThreadDispatcher auxDispatcher;
    private final ThreadDispatcher propDispatcher;
    private final ScheduledThreadDispatcher intDispatcher;
    private final ScheduledThreadDispatcher stableDispatcher;

    private final TimestampGenerator tsGen;
    private final UdpNetwork udpNetwork;

    private final Network proposeChannel;
    private final Network repliesChannel;
    private final Network stableChannel;
    private final Network otherChannel;


    private final ProcessDescriptor pd;
    private final int totalObjects;
    private final FailureDetector failureDetector;
    private Proposer proposer;
    private ConflictDetector cDetector;
    private DecideCallback callback;

    private Map<String, Pair<Integer, Integer>> barrierMap = new HashMap<String, Pair<Integer, Integer>>();
    private long pTotal = 0, pMax = 0, pCount;
    private long pdTotal = 0, pdMax = 0, pdCount;
    private long sdTotal = 0, sdMax = 0, sdCount;
    private long adTotal = 0, adMax = 0, adCount;

    public Caesar(int totalObjects) throws IOException {
        this.pd = ProcessDescriptor.getInstance();

        this.auxDispatcher = new ThreadDispatcher("AuxDispatcher", pd.auxThreads);
        this.cReqDispatcher = new ScheduledThreadDispatcher("CliReqDispatcher", pd.cReqThreads);
        this.intDispatcher = new ScheduledThreadDispatcher("IntDispatcher", pd.intThreads);
        this.propDispatcher = new ThreadDispatcher("ProposalDispatcher", pd.proposalThreads);
        this.stableDispatcher = new ScheduledThreadDispatcher("StableDispatcher", pd.stableThreads);

        this.udpNetwork = new UdpNetwork();
        if (pd.network.equals("TCP")) {
            proposeChannel = new TcpNetwork(0);
            repliesChannel = new TcpNetwork(1);
            stableChannel = new TcpNetwork(2);
            otherChannel = new TcpNetwork(3);
        } else {
            throw new IllegalArgumentException("Unknown network type: " + pd.network +
                    ". Check paxos.properties configuration.");
        }
        logger.info("Network: " + proposeChannel.getClass().getCanonicalName());
        logger.info("Network: " + repliesChannel.getClass().getCanonicalName());
        logger.info("Network: " + stableChannel.getClass().getCanonicalName());

        failureDetector = new FailureDetector(this, udpNetwork);

        this.totalObjects = totalObjects;

        this.tsGen = new TimestampGenerator(pd.localId, pd.numReplicas);
        this.cDetector = new ConflictDetector(totalObjects);

        this.proposer = new Proposer(tsGen, cDetector, proposeChannel, repliesChannel, stableChannel, otherChannel, intDispatcher, this);

//        QueueMonitor.getInstance().registerQueue("Aux Runnables", auxDispatcher.getQueue());
//        QueueMonitor.getInstance().registerQueue("CReq Runnables", cReqDispatcher.getQueue());
//        QueueMonitor.getInstance().registerQueue("Int Runnables", intDispatcher.getQueue());
//        QueueMonitor.getInstance().registerQueue("Prop Runnables", propDispatcher.getQueue());
//        QueueMonitor.getInstance().registerQueue("Stable Runnables", stableDispatcher.getQueue());
//
//        System.out.println("Queue Init Size: " + auxDispatcher.getQueue().remainingCapacity());
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
        proposeChannel.start();
        repliesChannel.start();
        stableChannel.start();
        otherChannel.start();

//        failureDetector.start();
    }

    public void deliver(Request request, Queue<Runnable> deliverQ) {
        this.callback.deliver(request, deliverQ);
    }

    public void propose(final Request request) {
//        long start = System.currentTimeMillis();

        cReqDispatcher.execute(() -> proposer.fastPropose(request));

//        long duration = System.currentTimeMillis() - start;
//
//        pTotal += duration;
//        pMax = Math.max(duration, pMax);
//        pCount++;
//        if(pCount % 5000 == 0) {
//            System.out.println(String.format("PE: Avg: %f; Max %d ", pTotal * 1.0 / pCount, pMax));
//            pCount = 0;
//            pTotal = 0;
//        }
    }

    public void onDelivery(Request request, Queue<Runnable> postDelQ) {
        proposer.onDelivery(request, postDelQ);
    }

    public void refresh() {
        cDetector = new ConflictDetector(totalObjects);
        this.proposer = new Proposer(tsGen, cDetector, proposeChannel, repliesChannel,
                stableChannel, otherChannel, intDispatcher, this);
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
                otherChannel.sendToAll(barrierPackage);
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
        auxDispatcher.submit(() -> proposer.startRecovery(nodeId));
    }

    private final class MessageHandlerImpl implements MessageHandler {
        public void onMessageReceived(Message msg, int sender) {
            logger.trace("Msg rcv: " + msg);
            MessageEvent event = new MessageEvent(msg, sender);

            PID process = ProcessDescriptor.getInstance().getProcess(sender);
            int priority = process.getPriority();


            if (msg.getType() == MessageType.FastPropose
                    || msg.getType() == MessageType.SlowPropose
                    || msg.getType() == MessageType.Retry) {

//                long start = System.currentTimeMillis();

                propDispatcher.execute(event, priority);

//                long duration = System.currentTimeMillis() - start;
//
//                pdTotal += duration;
//                pdMax = Math.max(duration, pdMax);
//                pdCount++;
//                if(pdCount % 5000 == 0) {
//                    System.out.println(String.format("PD: Avg: %f; Max %d ", pdTotal * 1.0 / pdCount, pdMax));
//                    pdCount = 0;
//                    pdTotal = 0;
//                }

            } else if (msg.getType() == MessageType.Stable) {
//                long start = System.currentTimeMillis();

                stableDispatcher.submit(event);

//                long duration = System.currentTimeMillis() - start;
//
//                sdTotal += duration;
//                sdMax = Math.max(duration, sdMax);
//                sdCount++;
//                if(sdCount % 5000 == 0) {
//                    System.out.println(String.format("SD: Avg: %f; Max %d ", sdTotal * 1.0 / sdCount, sdMax));
//                    sdCount = 0;
//                    sdTotal = 0;
//                }
            } else {
//                long start = System.currentTimeMillis();

                auxDispatcher.execute(event, priority);

//                long duration = System.currentTimeMillis() - start;
//
//                adTotal += duration;
//                adMax = Math.max(duration, pdMax);
//                adCount++;
//                if(adCount % 5000 == 0) {
//                    System.out.println(String.format("AD: Avg: %f; Max %d ", adTotal * 1.0 / adCount, adMax));
//                    adCount = 0;
//                    adTotal = 0;
//                }
            }


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

//                    case Alive:
//                        logger.trace("Alive message received");
//                        //TODO: Implement Handler
//                        break;

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
