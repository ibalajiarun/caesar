package hyflow.main;

import hyflow.benchmark.AbstractService;
import hyflow.caesar.Caesar;
import hyflow.caesar.replica.Replica;
import hyflow.caesar.statistics.RequestStats;
import hyflow.common.*;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.Random;
import java.util.Vector;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by balajiarun on 3/7/16.
 */
public class ClientManager implements Client {

    private static final Logger logger = LogManager.getLogger(ClientManager.class);
    private static final Marker marker = MarkerManager.getMarker("ClientManager");

    private final Semaphore finishedLock = new Semaphore(1);
    private final int localId;
    private int replicaId;
    private final AbstractService service;

    private final Caesar caesar;
    private final int numReplicas;
    private final IdGenerator idGenerator;
    private final Map<RequestId, Request> requestMap;

    private Vector<ClientThread> clients = new Vector<>();
    private AtomicInteger runningClients = new AtomicInteger(0);

    private AtomicInteger reqDoneCount = new AtomicInteger(0);

    private long startTime;
    private int lastRequestCount;
    private int barrierCount;

    public ClientManager(int replicaId, AbstractService service, Caesar caesar) throws IOException {
        this.replicaId = replicaId;
        this.service = service;
        this.caesar = caesar;
        this.localId = replicaId;

        this.numReplicas = ProcessDescriptor.getInstance().numReplicas;
        this.idGenerator = new SimpleIdGenerator(replicaId, numReplicas);

        this.requestMap = new ConcurrentHashMap<>(10000);

        new MonitorThread().start();
    }

    private static void printUsage() {
        System.out.println("bye");
        System.out.println("kill");
        System.out.println("<clientCount> <requestsPerClient> <conflict%> <writePercent%> <batchSize>");
    }

    @Override
    public void run() {
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        printUsage();
        while (true) {
            String line = null;
            try {
                line = reader.readLine();
            } catch (IOException e) {
                e.printStackTrace();
            }

            if (line == null) {
                break;
            }

            String[] args = line.split(" ");

            if (args[0].equals("gc")) {
                gc();
                continue;
            }

            if (args[0].equals("bye")) {
                break;
            }

            if (args[0].equals("kill")) {
                for (ClientThread client : clients) {
                    client.interrupt();
                }
                break;
            }

            if (args.length != 5) {
                System.err.println("Wrong command length! Expected:");
                printUsage();
                continue;
            }

            int clientCount;
            int requests;
            int conflictPercent, writePercent, batchSize;

            try {
                clientCount = Integer.parseInt(args[0]);
                requests = Integer.parseInt(args[1]);
                conflictPercent = Integer.parseInt(args[2]);
                writePercent = Integer.parseInt(args[3]);
                batchSize = Integer.parseInt(args[4]);
            } catch (NumberFormatException e) {
                System.err.println("Wrong argument! Expected:");
                printUsage();
                continue;
            }

            try {
                execute(clientCount, requests, conflictPercent, writePercent, batchSize);
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private void gc() {
        caesar.enterBarrier("pause", numReplicas);
        caesar.refresh();
        System.gc();
        caesar.enterBarrier("refresh", numReplicas);
        System.out.println("refreshed");
    }

    private void finished() {

        long duration = System.currentTimeMillis() - startTime;
        System.err.println(String.format("Finished %d reqs in %d ms; Tps %4.2f\n",
                lastRequestCount, duration,
                (double) lastRequestCount * 1000 / duration));

        RequestStats.getInstance().printAndResetStats();

        printUsage();

        conflictCount = 0;

        finishedLock.release();
    }

    private void execute(int clientCount, int requests, int conflictPercent, int writePercent, int batchSize)
            throws IOException, InterruptedException {

        finishedLock.acquire();

        barrierCount++;

        for (int i = clients.size(); i < clientCount; i++) {
            ClientThread client = new ClientThread(idGenerator.next());
            client.start();
            clients.add(client);
        }

        runningClients.addAndGet(clientCount);

        startTime = System.currentTimeMillis();
        lastRequestCount = clientCount * requests;

        for (int i = 0; i < clientCount; i++) {
            clients.get(i).execute(clientCount, requests, conflictPercent, writePercent, batchSize);
        }
    }

    int conflictCount = 0;

    @Override
    public void notifyClient(Request request) {
        Request req = requestMap.remove(request.getId());
//        if(logger.isDebugEnabled() && req.getObjectIds()[0] == 0) {
//            logger.debug("ReqTime: {} PD: {} RD {}", request.getId(), request.onProposeDuration, request.onRetryDuration);
//        }
        if (req != null) {

            RequestId rId = req.getId();
            reqDoneCount.incrementAndGet();
            req.setStatus(RequestStatus.Delivered);

            if (req.getObjectIds()[0] == 0) {
                conflictCount++;
            }

            synchronized (rId) {
                rId.notifyAll();
            }
        }
    }

    class MonitorThread extends Thread {

        @Override
        public void run() {
            int interval = ProcessDescriptor.getInstance().monitorInterval;
            int prevCount = 1, count;
            while (true) {

                try {
                    Thread.sleep(interval);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                count = reqDoneCount.getAndSet(0);
                if (count == 0 && prevCount == 0) {
                    continue;
                }

                System.out.println("Throughput: " + count * 1000.0 / interval);
                System.out.println("Conflict: " + conflictCount);
                prevCount = count;

            }

        }
    }

    class ClientThread extends Thread {
        private final int clientId;
        private ArrayBlockingQueue<Integer> sends;
        private int clientCount;
        private int conflictPercent;
        private int writePercent;
        private int batchSize;

        private Random random;

        ClientThread(int clientId) throws IOException {
            this.clientId = clientId;
            this.random = new Random(replicaId * clientId);
            this.sends = new ArrayBlockingQueue<>(128);
        }

        @Override
        public void run() {
            try {
                IdGenerator seqGen;

                Integer count;
                boolean read, conflict;
                int accessMode;
                Vector<Request> requests;

                while (true) {
                    count = sends.take();
                    requests = new Vector<>();
                    seqGen = new SimpleIdGenerator(clientId, clientCount);

                    long start = System.currentTimeMillis();

                    for (int i = 0; i < count; i += batchSize) {

                        Request request;

                        read = random.nextInt(100) >= writePercent;

                        if (conflictPercent != -1) {
                            conflict = random.nextInt(100) < conflictPercent;
                            accessMode = conflict ? 0 : 1;
                        } else {
                            accessMode = 2;
                        }

                        request = service.createRequest(new RequestId(localId, seqGen.next()),
                                read, accessMode, batchSize, clientCount * numReplicas);

                        RequestId requestId = request.getId();
                        requestMap.put(requestId, request);
                        requests.add(request);

                        caesar.propose(request);

                        if (i % 100 == 0) {
                            Thread.sleep(ProcessDescriptor.getInstance().proposerSleep);
                        }

                    }
//                    Thread.sleep(100);
                    if (ProcessDescriptor.getInstance().localId == 2) {
//                        System.exit(0);
                    }
                    for (Request request : requests) {
                        RequestId requestId = request.getId();
                        synchronized (requestId) {
                            int times = 0;
                            while (request.getStatus() != RequestStatus.Delivered) {
                                requestId.wait(1000);
                                times++;
                                if (times % 10 == 0) {
                                    if (logger.isInfoEnabled())
                                        logger.info(marker, "Too long {};", request);
                                }
                            }
                            assert request.getStatus() == RequestStatus.Delivered : "Not Delivered " + request;
                        }
                    }

                    long duration = System.currentTimeMillis() - start;
                    System.err.println(String.format("Client Finished %d %d %4.2f\n", clientId, duration,
                            (double) count * 1000 / duration));

                    int stillActive = runningClients.decrementAndGet();
                    if (stillActive == 0) {
                        finished();
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        void execute(int clientCount, int count, int conflictPercent, int write, int batchSize) throws InterruptedException {
            System.out.println(String.format("Executing %d %d %d %d %d", clientCount, count, conflictPercent, write, batchSize));
            this.clientCount = clientCount;
            this.conflictPercent = conflictPercent;
            this.writePercent = write;
            this.batchSize = batchSize;
            this.sends.put(count);
        }

    }

}
