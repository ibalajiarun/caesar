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
    private final short localId;
    private Vector<ClientThread> clients = new Vector<>();
    private AtomicInteger runningClients = new AtomicInteger(0);

    private AtomicInteger reqDoneCount = new AtomicInteger(0);
    private MonitorThread monitorThread;

    public LatencyClient(short replicaId, AbstractService service, Caesar caesar) throws IOException {
        this.service = service;
        this.caesar = caesar;

        this.numReplicas = ProcessDescriptor.getInstance().numReplicas;
        this.localId = replicaId;

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

        for (int client : clients) {
            for (int conflict : conflicts) {
                for (int request : requests) {
                    try {
                        execute(client, request, conflict, writePercent, batchSize);
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
//        monitorThread.setDone();
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        gc();
        try {
            Thread.sleep(4000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        caesar.enterBarrier("resume", numReplicas);
        System.out.println("Resumed");
        finishedLock.release();
    }

    private void execute(int clientCount, int requests, int conflictPercent, int writePercent, int batchSize)
            throws IOException, InterruptedException {

        finishedLock.acquire();

//        monitorThread = new MonitorThread("latlogs/tps-C" + conflictPercent + "-R" + requests + ".log");

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

//        monitorThread.start();
    }

    @Override
    public void notifyClient(Request request) {
        Request req = requestMap.get(request.getId());
        if (req != null) {
            RequestId rId = req.getId();
            reqDoneCount.incrementAndGet();
            synchronized (rId) {
                req.setStatus(RequestStatus.Delivered);
                rId.notifyAll();
            }
        }
    }

    private class MonitorThread extends Thread {

        volatile boolean done = false;
        private String filename;

        public MonitorThread(String filename) {
            this.filename = filename;
        }

        @Override
        public void run() {
            int interval = ProcessDescriptor.getInstance().monitorInterval;
            int prevCount = 1, count;
            File file = new File(filename);
            if (file.exists()) {
                file.delete();
            }
            file.getParentFile().mkdirs();
            FileWriter fw = null;
//            int iterations = 0;
            try {
                file.createNewFile();
                fw = new FileWriter(file.getAbsoluteFile());

                BufferedWriter bw = new BufferedWriter(fw);
                while (!done) {

                    Thread.sleep(interval);

                    count = reqDoneCount.getAndSet(0);
                    if (count == 0 && prevCount == 0) {
                        continue;
                    }

//                    iterations++;
//                    if(iterations > 20 && localId == 0) {
//                        System.out.println("KILLING");
//                        System.exit(-1);
//                    }
                    double tps = count * 1000.0 / interval;
                    bw.write(tps + "\n");
//                    if(iterations > 50) {
//                        bw.flush();
//                        System.exit(-1);
//                    }
                    System.out.println("Throughput: " + tps);
                    prevCount = count;
                }
                bw.close();
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(-1);
            }
        }

        public void setDone() {
            this.done = true;
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
            this.random = new Random(localId * clientId);
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

//                    if(clientId >= 500) {
//                        Thread.sleep(20000);
//                    }

                    seqGen = new SimpleIdGenerator(clientId, clientCount);
                    file = new File("latlogs/latency-Client-" + clientCount + "C" + conflictPercent + "-R" + count + "/" + clientId + ".log");
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
                        bw.flush();
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
