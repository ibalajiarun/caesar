package hyflow.main;

import hyflow.benchmark.AbstractService;
import hyflow.caesar.Caesar;
import hyflow.caesar.network.Network;
import hyflow.caesar.replica.Replica;
import hyflow.common.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.ZMQ;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by balajiarun on 3/7/16.
 */
public class ClientManager {

    private static final Logger logger = LogManager.getLogger(ClientManager.class);
    private final AbstractService service;
    private final int clientCount;
    private final int replicaId;
    private final Replica replica;
    private final ClientThread[] clients;
    private final ConcurrentMap<RequestId, RequestId> requestMap;
    private final Caesar caesar;
    private final Network network;
    private final int numReplicas;
    private AtomicInteger count = new AtomicInteger(0);
    private AtomicInteger latency = new AtomicInteger(0);

    public ClientManager(int clientCount, int replicaId, AbstractService service, Replica replica, Caesar caesar) throws IOException {
        this.service = service;
        this.clientCount = clientCount;
        this.replicaId = replicaId;
        this.replica = replica;
        this.caesar = caesar;
        this.network = caesar.getNetwork();

        this.clients = new ClientThread[clientCount];
        this.numReplicas = ProcessDescriptor.getInstance().numReplicas;
        IdGenerator generator = new SimpleIdGenerator(replicaId, numReplicas);
        for (int i = 0; i < clientCount; i++) {
            clients[i] = new ClientThread(generator.next());
        }

        this.requestMap = new ConcurrentHashMap<>(clientCount);
    }

    public void start() {
        for (int i = 0; i < clientCount; i++) {
            clients[i].start();
        }
    }

    public void pause() {
        for (int i = 0; i < clientCount; i++) {
            clients[i].pause();
        }
        for (int i = 0; i < clientCount; i++) {
            clients[i].waitUntilPaused();
        }
    }

    public void proceed() {
        for (int i = 0; i < clientCount; i++) {
            clients[i].proceed();
        }
    }

    public void collectStats(int sleepTime) {
        ZMQ.Context context = ZMQ.context(1);
        ZMQ.Socket sender = context.socket(ZMQ.PUSH);
        String host = ProcessDescriptor.getInstance().zmqHost;
        String port = ProcessDescriptor.getInstance().zmqPort;
        sender.connect("tcp://" + host + ":" + port);

        new Thread(() -> {
//            int iter = 0;
            start();
            while (true) {
                try {
                    Thread.sleep(sleepTime);

//                    logger.fatal("pausing");
                    pause();
//                    logger.fatal("pause barrier");
                    caesar.enterBarrier("pause", numReplicas);
//                    logger.fatal("Paused");

                    int localLatency = latency.getAndSet(0);
                    int localCount = count.getAndSet(0);
                    if (localCount > 0) {
                        double gtps = localCount / (sleepTime * 0.001);
                        logger.fatal("Tps: " + gtps);

                        double lat = localLatency / localCount;

                        byte[] out = ("c:" + Integer.toString((int) gtps) + ":" + Integer.toString((int) lat)).getBytes();
                        sender.send(out, 0);
                    }

//                    iter++;
//                    if(iter > 5) {
//                        logger.fatal("refresh barrier");
//                        caesar.enterBarrier("refresh", numReplicas);
//                        logger.fatal("refeshing");
//
//                        caesar.refresh();
//
//                        logger.fatal("resume barrier");
//                        caesar.enterBarrier("resume", numReplicas);
//                        logger.fatal("resumed");
//                        iter = 0;
//                    }

                    caesar.refresh();
                    System.gc();
                    System.gc();

//                    logger.fatal("refresh barrier");
                    caesar.enterBarrier("refresh", numReplicas);
                    logger.fatal("refreshed");

                    proceed();

                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

        }).start();

    }

    public void notifyForReq(Request request) {
        RequestId rId = requestMap.get(request.getId());
//        logger.fatal("notifying " + rId);
        if (rId != null) {
            count.incrementAndGet();
//            logger.fatal("notifying " + rId);
            synchronized (rId) {
                rId.notifyAll();
            }
        }
    }

    class ClientThread extends Thread {

        private final int clientId;

        private int sequenceNum = 0;

        private int requestCount = 0;
        private int aggregateLatency = 0;

        private AtomicBoolean pause = new AtomicBoolean(false);
        private AtomicBoolean paused = new AtomicBoolean(false);

        public ClientThread(int clientId) {
            this.clientId = clientId;
        }

        @Override
        public void run() {
            long start, elapsed;
            int count;

            try {
                while (!Thread.interrupted()) {

                    while (pause.get()) {
                        paused.getAndSet(true);
                        yield();
                    }
                    paused.getAndSet(false);

                    Request request = service.createRequest(new RequestId(clientId, sequenceNum++), clientCount * numReplicas, false);
                    RequestId requestId = request.getId();

                    count = 0;

                    start = System.currentTimeMillis();

                    requestMap.put(requestId, requestId);

                    synchronized (requestId) {
                        replica.submit(request);
                        while (request.getStatus() != RequestStatus.Delivered) {
                            requestId.wait(100);
                            count++;
                            if (count % 20 == 0) {
                                logger.fatal("Too long " + request);
                                break;
                            }
                        }
//                        assert request.getStatus() == RequestStatus.Delivered : "Not Delivered" + request;
                    }
                    elapsed = System.currentTimeMillis() - start;

                    if (count > 5)
                        logger.fatal("TOOK " + elapsed + " FOR " + requestId);

                    latency.addAndGet((int) elapsed);
                }

            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        public void pause() {
            pause.getAndSet(true);
        }

        public void waitUntilPaused() {
            while (!paused.get()) ;
//            logger.fatal("Client ID:" + clientId + "Seq:" + sequenceNum);
        }

        public void proceed() {
            pause.getAndSet(false);
        }

        public int getLatency() {
            return aggregateLatency / requestCount;
        }

        public int getRequestCount() {
            int ret = requestCount;
            requestCount = 0;
            return ret;
        }

    }

}
