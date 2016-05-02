package hyflow.main;

import hyflow.benchmark.AbstractService;
import hyflow.caesar.Caesar;
import hyflow.caesar.replica.Replica;
import hyflow.caesar.statistics.RequestStats;
import hyflow.common.*;
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
public class ClientManager {

    private static final Logger logger = LogManager.getLogger(ClientManager.class);
    private static final Marker marker = MarkerManager.getMarker("ClientManager");

    private final Semaphore finishedLock = new Semaphore(1);
    private final AbstractService service;
    private final Replica replica;
    private final Caesar caesar;
    private final int numReplicas;
    private final IdGenerator idGenerator;
    private final Map<RequestId, RequestId> requestMap;

    private Vector<ClientThread> clients = new Vector<>();
    private AtomicInteger runningClients = new AtomicInteger(0);

    private long startTime;
    private int lastRequestCount;
    private int barrierCount;

    ClientManager(int replicaId, AbstractService service, Replica replica, Caesar caesar) throws IOException {
        this.service = service;
        this.replica = replica;
        this.caesar = caesar;

        this.numReplicas = ProcessDescriptor.getInstance().numReplicas;
        this.idGenerator = new SimpleIdGenerator(replicaId, numReplicas);

        this.requestMap = new ConcurrentHashMap<>();
    }

    private static void printUsage() {
        System.out.println("bye");
        System.out.println("kill");
        System.out.println("<clientCount> <requestsPerClient> <conflict%> <writePercent%> <batchSize>");
    }

    public void run() throws IOException, InterruptedException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        printUsage();
        while (true) {
            String line = reader.readLine();
            if (line == null) {
                break;
            }

            String[] args = line.split(" ");

            if (args[0].equals("bye")) {
                break;
            }

            if (args[0].equals("kill")) {
                for (ClientThread client : clients) {
                    client.interrupt();
                }
                continue;
            }

            if (args.length != 4) {
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

            execute(clientCount, requests, conflictPercent, writePercent, batchSize);
        }
    }

    private void finished() {

        long duration = System.currentTimeMillis() - startTime;
        System.err.println(String.format("Finished %d reqs in %d ms; Tps %4.2f\n",
                lastRequestCount, duration,
                (double) lastRequestCount * 1000 / duration));

        RequestStats.getInstance().printAndResetStats();
        finishedLock.release();

        printUsage();
    }

    private void execute(int clientCount, int requests, int conflictPercent, int writePercent, int batchSize)
            throws IOException, InterruptedException {

        finishedLock.acquire();

        caesar.enterBarrier("pause" + barrierCount, numReplicas);
        caesar.refresh();
        System.gc();
        System.gc();
        caesar.enterBarrier("refresh" + barrierCount, numReplicas);
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

    public void notifyClient(Request request) {
        RequestId rId = requestMap.get(request.getId());
        if (rId != null) {
            synchronized (rId) {
                rId.notifyAll();
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
            this.random = new Random();
            this.sends = new ArrayBlockingQueue<>(128);
        }

        @Override
        public void run() {
            try {
                int sequenceNum = 0;

                Integer count;
                boolean read, conflict;
                int accessMode;
                Vector<Request> requests;

                while (true) {
                    count = sends.take();
                    requests = new Vector<>();

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

                        request = service.createRequest(new RequestId(clientId, sequenceNum++),
                                read, accessMode, batchSize, clientCount * numReplicas);

                        RequestId requestId = request.getId();
                        requestMap.put(requestId, requestId);
                        requests.add(request);

                        replica.submit(request);

                    }
                    Thread.sleep(100);
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
                                    logger.info(marker, "Too long " + request);
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
            this.clientCount = clientCount;
            this.conflictPercent = conflictPercent;
            this.writePercent = write;
            this.batchSize = batchSize;
            this.sends.put(count);
        }

    }

}
