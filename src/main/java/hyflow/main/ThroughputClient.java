package hyflow.main;

import hyflow.benchmark.AbstractService;
import hyflow.benchmark.kv.KeyValue;
import hyflow.caesar.Caesar;
import hyflow.caesar.messages.FastProposeReply;
import hyflow.caesar.statistics.RequestStats;
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
public class ThroughputClient implements Client {

    private static final Logger logger = LogManager.getLogger(ThroughputClient.class);
    private static final Marker marker = MarkerManager.getMarker("ThroughputClient");
    protected final Properties configuration = new Properties();
    private final Semaphore finishedLock = new Semaphore(1);
    private final AbstractService service;
    private final Caesar caesar;
    private final int numReplicas;
    private final short localId;
    private final Map<RequestId, Request> requestMap;
    private Vector<ClientThread> clients = new Vector<>();
    private AtomicInteger runningClients = new AtomicInteger(0);
    private AtomicInteger reqDoneCount = new AtomicInteger(0);
    private MonitorThread monitorThread;


    public ThroughputClient(short replicaId, AbstractService service, Caesar caesar) throws IOException {
        this.service = service;
        this.caesar = caesar;

        this.numReplicas = ProcessDescriptor.getInstance().numReplicas;
        this.localId = replicaId;

        this.requestMap = new ConcurrentHashMap<>();

        InputStream fis = Paths.get("tpsclient.properties").toUri().toURL().openStream();
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

    private class CostInfo {
        long retry, propose, deliver, count;
        int wait, waitCount;
    }

    private void finished(int conflictPercent, int reqCount, double tps) {
        logger.fatal("Finished");
        System.out.println("Finished");
        monitorThread.setDone();
        RequestStats.getInstance().printAndResetStats();
        File file = new File("costlogs/cost-C" + conflictPercent + "-R" + reqCount + ".log");
        if (file.exists()) {
            file.delete();
        }
        file.getParentFile().mkdirs();
        FileWriter fw = null;
        try {
            file.createNewFile();
            fw = new FileWriter(file.getAbsoluteFile());

            BufferedWriter bw = new BufferedWriter(fw);
            CostInfo info = new CostInfo();
            requestMap.forEach((rId, request) -> {
                info.propose += request.proposeDuration;
                info.retry += request.retryDuration;
                info.deliver += request.deliverDuration;
                if (request.info != null) {
                    FastProposeReply[] replies = request.info.getReplies();
                    if (request.objectIds[0] < ProcessDescriptor.getInstance().conflictPool) {
                        int max = 0;
                        for (FastProposeReply reply : replies) {
                            if (reply != null) {
                                max = Math.max(max, reply.getWaitTime());
                            }
                        }
                        info.wait += max;
                        info.waitCount++;
                    }
                }
                info.count++;
            });
            bw.write(String.format("%f,%f,%f,%f,%f\n",
                tps,
                info.propose * 1.0 / info.count,
                info.retry * 1.0 / info.count,
                info.deliver * 1.0 / info.count,
                info.wait * 1.0 / info.waitCount));
            bw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        requestMap.clear();


        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        gc();
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        finishedLock.release();
    }

    private void execute(int clientCount, int requests, int conflictPercent, int writePercent, int batchSize)
    throws IOException, InterruptedException {

        finishedLock.acquire();

        monitorThread = new MonitorThread("tpslogs/tps-C" + conflictPercent + "-R" + requests + ".log");

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

        monitorThread.start();
    }

    @Override
    public void notifyClient(Request request) {
        Request req = requestMap.get(request.getId());
        if (req != null) {
            RequestId rId = req.getId();
            reqDoneCount.incrementAndGet();
            req.setStatus(RequestStatus.Delivered);

            req.deliverDuration = request.deliverDuration;
            req.proposeDuration = request.proposeDuration;
            req.retryDuration = request.retryDuration;
            req.info = request.info;

            synchronized (rId) {
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
            try {
                file.createNewFile();
                fw = new FileWriter(file.getAbsoluteFile());

                BufferedWriter bw = new BufferedWriter(fw);
                while (!done) {

                    Thread.sleep(interval);


                    count = reqDoneCount.getAndSet(0);
//                    if (count == 0 && prevCount == 0) {
//                        continue;
//                    }

                    double tps = (count * 1000.0) / (double) interval;
                    bw.write(tps + "\n");
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
            this.random = new Random(localId * clientId * System.nanoTime());
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

                    if (batchSize > 1) {
                        Request request;
                        for (int i = 0; i < count; i += batchSize) {
                            request = ((KeyValue)service).createRequest(new RequestId(localId, seqGen.next()),
                                conflictPercent, batchSize, numReplicas, random);

                            RequestId requestId = request.getId();
                            requestMap.put(requestId, request);

                            caesar.propose(request);
                            requests.add(request);

                            if (i % 100 == 0) {
                                Thread.sleep(ProcessDescriptor.getInstance().proposerSleep);
                            }
                            
                        }

                    } else {
                        Request request;

                        for (int i = 0; i < count; i += batchSize) {


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

                            caesar.propose(request);
                            requests.add(request);

                            if (i % 100 == 0) {
                                Thread.sleep(ProcessDescriptor.getInstance().proposerSleep);
                            }

                        }
                    }
                    for (Request request : requests) {
                        RequestId requestId = request.getId();
                        synchronized (requestId) {
                            int times = 0;
                            while (request.getStatus() != RequestStatus.Delivered) {
                                requestId.wait(5000);
                                times++;
                                System.out.println(request);
                                if (times % 1000 == 0) {
                                    if (logger.isInfoEnabled())
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
                        finished(conflictPercent, count, (double) count * 1000 / duration);
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
