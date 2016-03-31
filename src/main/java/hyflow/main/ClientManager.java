package hyflow.main;

import hyflow.caesar.replica.Replica;
import hyflow.common.*;
import hyflow.service.Service;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zeromq.ZMQ;

import java.io.IOException;

/**
 * Created by balajiarun on 3/7/16.
 */
public class ClientManager {

    private static final Logger logger = LogManager.getLogger(ClientManager.class);
    private final Service service;
    private final int clientCount;
    private final int replicaId;
    private final Replica replica;
    private final ClientThread[] clients;
    private int count = 0;

    public ClientManager(int clientCount, int replicaId, Service service, Replica replica) throws IOException {
        this.service = service;
        this.clientCount = clientCount;
        this.replicaId = replicaId;
        this.replica = replica;

        this.clients = new ClientThread[clientCount];
        IdGenerator generator = new SimpleIdGenerator(replicaId, ProcessDescriptor.getInstance().numReplicas);
        for (int i = 0; i < clientCount; i++) {
            clients[i] = new ClientThread(generator.next());
        }
    }

    public void start() {
        for (int i = 0; i < clientCount; i++) {
            clients[i].start();
        }
    }

    public void stop() {
        for (int i = 0; i < clientCount; i++) {
            clients[i].interrupt();
        }
    }

    public void collectStats(int sleepTime) {
        ZMQ.Context context = ZMQ.context(1);
        ZMQ.Socket sender = context.socket(ZMQ.PUSH);
        sender.connect("tcp://localhost:5558");

        new Thread(() -> {
            start();
            while (true) {
                try {
                    Thread.sleep(sleepTime);

                    double gtps = count / (sleepTime * 0.001);
                    logger.fatal("Tps: " + gtps);
                    count = 0;

                    byte[] out = Integer.toString((int) gtps).getBytes();
                    sender.send(out, 0);

                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

        }).start();

    }

    public void upCount() {
        count++;
    }

    class ClientThread extends Thread {

        private final int clientId;

        private int sequenceNum = 0;

        private int requestCount = 0;
        private int aggregateLatency = 0;

        private volatile boolean pause = false;
        private volatile boolean paused = false;

        public ClientThread(int clientId) {
            this.clientId = clientId;
        }

        @Override
        public void run() {
            long start, elapsed;

            try {
                while (!Thread.interrupted()) {

//                    while(pause) {
//                        paused = true;
//                    }
//                    paused = false;

                    Request request = service.createRequest(new RequestId(clientId, sequenceNum++), false);
                    RequestId requestId = request.getId();

//                    start = System.currentTimeMillis();
                    synchronized (requestId) {
                        replica.submit(request);
//                        while(request.getStatus() != RequestStatus.Delivered)
                        requestId.wait();
                    }
//                    elapsed = System.currentTimeMillis() - start;

                    requestCount++;
//                    aggregateLatency += (int) elapsed;
                }

            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        public void pause() {
            pause = true;
            while (!paused) ;
        }

        public void proceed() {
            pause = false;
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
