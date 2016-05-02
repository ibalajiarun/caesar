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

/**
 * Created by balajiarun on 3/7/16.
 */
public class ClientManagerPeriodic {

    private static final Logger logger = LogManager.getLogger(ClientManagerPeriodic.class);
    private final AbstractService service;
    private final int clientCount;
    private final int replicaId;
    private final Replica replica;
    private final ClientThread[] clients;
    private final ConcurrentMap<RequestId, RequestId> requestMap;
    private final Caesar caesar;
    private final Network network;
    private final int numReplicas;
    private final int reqType;
    private final int batchSize;

//    private AtomicInteger count = new AtomicInteger(0);
//    private AtomicInteger latency = new AtomicInteger(0);

    public ClientManagerPeriodic(int clientCount, int replicaId, AbstractService service, Replica replica, Caesar caesar) throws IOException {
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
        this.reqType = 0;
        this.batchSize = 1;
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

    public int aggregateTime() {
        int time = 0;
        for (int i = 0; i < clientCount; i++) {
            time += clients[i].elapsedTime;
            clients[i].elapsedTime = 0;
        }
        return time;
    }

    public int aggregateCount() {
        int count = 0;
        for (int i = 0; i < clientCount; i++) {
            count += clients[i].requestCount;
            clients[i].requestCount = 0;
        }
        return count;
    }

    public void collectStats(int sleepTime) {
        ZMQ.Context context = ZMQ.context(1);
        ZMQ.Socket sender = context.socket(ZMQ.PUSH);
        String host = ProcessDescriptor.getInstance().zmqHost;
        String port = ProcessDescriptor.getInstance().zmqPort;
        sender.connect("tcp://" + host + ":" + port);

        new Thread(() -> {
            int count = 0;
            start();
            while (true) {
                try {
                    Thread.sleep(sleepTime);

                    pause();

                    logger.fatal("Pause barrier");
                    caesar.enterBarrier("pause" + count, numReplicas);
                    logger.fatal("Paused");
//                    int localLatency = latency.getAndSet(0);
//                    int localCount = count.getAndSet(0);
                    int totalTime = aggregateTime();
                    int totalCount = aggregateCount();
                    if (totalCount > 0) {
                        double gtps = totalCount / (sleepTime * 0.001);
                        logger.fatal("Tps: " + gtps);

                        double lat = totalTime / totalCount;

                        byte[] out = ("c:" + Integer.toString((int) gtps) + ":" + Integer.toString((int) lat)).getBytes();
                        sender.send(out, 0);
                    }

                    caesar.refresh();
                    System.gc();
                    System.gc();

                    logger.fatal("Refresh barrier");
                    caesar.enterBarrier("refresh" + count, numReplicas);
                    logger.fatal("refreshed");

                    count++;
                    proceed();

                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

        }).start();

    }

    public void notifyForReq(Request request) {
        RequestId rId = requestMap.get(request.getId());
        if (rId != null) {
            synchronized (rId) {
                rId.notifyAll();
            }
        }
    }

    class ClientThread extends Thread {

        private final int clientId;
        public int requestCount = 0;
        public int elapsedTime = 0;
        private int sequenceNum = 0;
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

                    Request request = service.createRequest(new RequestId(clientId, sequenceNum++), false, reqType, batchSize, clientCount * numReplicas);
                    RequestId requestId = request.getId();

                    count = 0;

                    start = System.currentTimeMillis();

                    requestMap.put(requestId, requestId);

                    synchronized (requestId) {
                        replica.submit(request);
                        while (request.getStatus() != RequestStatus.Stable) {
                            requestId.wait(100);
                            count++;
                            if (count % 5 == 0) {
                                logger.fatal("Too long " + request);
                            }
                        }
                        assert request.getStatus() == RequestStatus.Stable : "Not Delivered" + request;
                    }

                    elapsed = System.currentTimeMillis() - start;

                    if (count > 5)
                        logger.fatal("TOOK " + elapsed + " FOR " + requestId);

                    requestCount++;
                    elapsedTime += (int) elapsed;
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

    }

}
