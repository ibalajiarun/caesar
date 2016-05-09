package hyflow.main;

import hyflow.benchmark.AbstractService;
import hyflow.caesar.Caesar;
import hyflow.common.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;

import java.io.*;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by balajiarun on 3/7/16.
 */
public class LatencyClient implements Client {

    private static final Logger logger = LogManager.getLogger(LatencyClient.class);
    private static final Marker marker = MarkerManager.getMarker("LatencyClient");
    protected final Properties configuration = new Properties();
    private final Semaphore finishedLock = new Semaphore(1);
    private final AbstractService service;
    private final Caesar caesar;
    private final int numReplicas;
    //    private final IdGenerator idGenerator;
    private final Map<RequestId, Request> requestMap;
    private final int localId;
    private Vector<ClientThread> clients = new Vector<>();
    private AtomicInteger runningClients = new AtomicInteger(0);

    public LatencyClient(int replicaId, AbstractService service, Caesar caesar) throws IOException {
        this.service = service;
        this.caesar = caesar;

        this.numReplicas = ProcessDescriptor.getInstance().numReplicas;
        this.localId = ProcessDescriptor.getInstance().localId;
//        this.idGenerator = new SimpleIdGenerator(replicaId, numReplicas);

        this.requestMap = new ConcurrentHashMap<>();

        InputStream fis = Paths.get("latclient.properties").toUri().toURL().openStream();
        configuration.load(fis);
        fis.close();
    }

    @Override
    public void run() {
        String line;
        StringTokenizer st;
        int[] conflicts, clients, requests;
        int count;

        line = configuration.getProperty("Conflicts");
        st = new StringTokenizer(line, ",");
        count = st.countTokens();
        conflicts = new int[count];
        for (int i = 0; i < count; i++) {
            conflicts[i] = Integer.parseInt(st.nextToken());
        }

        line = configuration.getProperty("Clients");
        st = new StringTokenizer(line, ",");
        count = st.countTokens();
        clients = new int[count];
        for (int i = 0; i < count; i++) {
            clients[i] = Integer.parseInt(st.nextToken());
        }

        line = configuration.getProperty("Requests");
        st = new StringTokenizer(line, ",");
        count = st.countTokens();
        requests = new int[count];
        for (int i = 0; i < count; i++) {
            requests[i] = Integer.parseInt(st.nextToken());
        }

        int writePercent = Integer.parseInt(configuration.getProperty("WritePercent"));
        int batchSize = Integer.parseInt(configuration.getProperty("BatchSize"));

        for (int i = 0; i < clients.length; i++) {
            for (int j = 0; j < conflicts.length; j++) {
                for (int k = 0; k < requests.length; k++) {
                    try {
                        execute(clients[i], requests[k], conflicts[j], writePercent, batchSize);
                    } catch (Exception e) {
                        e.printStackTrace();
                        System.exit(-1);
                    }
                }
            }
        }

        try {
            finishedLock.acquire();
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.exit(-1);
        }
        finishedLock.release();
        System.out.println("Done");
    }

    private void gc() {
        caesar.enterBarrier("pause", numReplicas);
        caesar.refresh();
        System.gc();
        caesar.enterBarrier("refresh", numReplicas);
        System.out.println("refreshed");
    }

    private void finished() {
        System.out.println("Finished");
        gc();
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        finishedLock.release();
    }

    private void execute(int clientCount, int requests, int conflictPercent, int writePercent, int batchSize)
            throws IOException, InterruptedException {

        finishedLock.acquire();

        System.out.println(String.format("Executing %d %d %d %d %d", clientCount, requests, conflictPercent, writePercent, batchSize));

        for (int i = clients.size(); i < clientCount; i++) {
            ClientThread client = new ClientThread(i);
            client.start();
            clients.add(client);
        }

        runningClients.addAndGet(clientCount);

        for (int i = 0; i < clientCount; i++) {
            clients.get(i).execute(clientCount, requests, conflictPercent, writePercent, batchSize);
        }
    }

    @Override
    public void notifyClient(Request request) {
        Request req = requestMap.get(request.getId());
        if (req != null) {
            RequestId rId = req.getId();
            synchronized (rId) {
                req.setStatus(RequestStatus.Delivered);
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
                int sequenceNum;

                Integer count;
                boolean read, conflict;
                int accessMode;
                File file;
                IdGenerator seqGen;

                while (true) {
                    count = sends.take();
                    seqGen = new SimpleIdGenerator(clientId, clientCount);
                    file = new File("latlogs/latency-C" + conflictPercent + "-R" + count + "/" + clientId + ".log");
                    if (file.exists()) {
                        file.delete();
                    }
                    file.getParentFile().mkdirs();
                    file.createNewFile();

                    FileWriter fw = new FileWriter(file.getAbsoluteFile());
                    BufferedWriter bw = new BufferedWriter(fw);

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
                                read, accessMode, batchSize, numReplicas);

                        RequestId requestId = request.getId();
                        requestMap.put(requestId, request);

                        long start = System.currentTimeMillis();

                        caesar.propose(request);

                        synchronized (requestId) {
                            while (request.getStatus() != RequestStatus.Delivered) {
                                requestId.wait(1000);
                            }
                        }

                        long duration = System.currentTimeMillis() - start;
                        bw.write(duration + "\n");
                    }

                    bw.close();

                    int stillActive = runningClients.decrementAndGet();
                    if (stillActive == 0) {
                        finished();
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (IOException e) {
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
