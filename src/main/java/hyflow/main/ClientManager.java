package hyflow.main;

import hyflow.benchmark.AbstractService;
import hyflow.caesar.Caesar;
import hyflow.caesar.replica.Replica;
import hyflow.common.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;
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

    public ClientManager(int replicaId, AbstractService service, Replica replica, Caesar caesar) throws IOException {
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
        System.out.println("<clientCount> <requestsPerClient> <reqType>");
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
                break;
            }

            if (args.length != 3) {
                System.err.println("Wrong command length! Expected:");
                printUsage();
                continue;
            }

            int clientCount;
            int requests;
            int type;

            try {
                clientCount = Integer.parseInt(args[0]);
                requests = Integer.parseInt(args[1]);
                type = Integer.parseInt(args[2]);
            } catch (NumberFormatException e) {
                System.err.println("Wrong argument! Expected:");
                printUsage();
                continue;
            }

            execute(clientCount, requests, type);
        }
    }

    public void finished() {

        long duration = System.currentTimeMillis() - startTime;
        System.err.println(String.format("Finished %d %4.2f\n", duration,
                (double) lastRequestCount / duration));

        caesar.enterBarrier("pause" + barrierCount, numReplicas);
        caesar.refresh();
        System.gc();
        System.gc();
        caesar.enterBarrier("refresh" + barrierCount, numReplicas);
        barrierCount++;

        finishedLock.release();
    }

    private void execute(int clientCount, int requests, int reqType)
            throws IOException, InterruptedException {

        finishedLock.acquire();

        for (int i = clients.size(); i < clientCount; i++) {
            ClientThread client = new ClientThread(idGenerator.next());
            client.start();
            clients.add(client);
        }

        runningClients.addAndGet(clientCount);

        startTime = System.currentTimeMillis();
        lastRequestCount = clientCount * requests;

        for (int i = 0; i < clientCount; i++) {
            clients.get(i).execute(clientCount, requests, reqType);
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
        private int reqType;

        ClientThread(int clientId) throws IOException {
            this.clientId = clientId;
            this.sends = new ArrayBlockingQueue<>(128);
        }

        @Override
        public void run() {
            try {
                int sequenceNum = 0;

                Integer count;
                Vector<Request> requests;

                while (true) {
                    count = sends.take();
                    requests = new Vector<>();

                    for (int i = 0; i < count; i++) {

                        Request request = service.createRequest(new RequestId(clientId, sequenceNum++),
                                false, reqType, clientCount * numReplicas);

                        RequestId requestId = request.getId();
                        requestMap.put(requestId, requestId);
                        requests.add(request);

                        replica.submit(request);

                    }
                    for (Request request : requests) {
                        RequestId requestId = request.getId();
                        synchronized (requestId) {
                            int times = 0;
                            while (request.getStatus() != RequestStatus.Stable) {
                                requestId.wait(100);
                                times++;
                                if (times % 5 == 0) {
                                    logger.fatal("Too long " + request);
                                }
                            }
                            assert request.getStatus() == RequestStatus.Stable : "Not Stable " + request;
                        }
                    }
                    int stillActive = runningClients.decrementAndGet();
                    if (stillActive == 0) {
                        finished();
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        public void execute(int clientCount, int count, int reqType) throws InterruptedException {
            this.clientCount = clientCount;
            this.reqType = reqType;
            this.sends.put(count);
        }

    }

}
