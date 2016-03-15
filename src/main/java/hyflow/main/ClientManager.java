package hyflow.main;

import hyflow.caesar.Caesar;
import hyflow.common.Request;
import hyflow.common.RequestId;
import hyflow.service.Service;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Random;
import java.util.Vector;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by balajiarun on 3/7/16.
 */
public class ClientManager {

    private Vector<ClientThread> clients = new Vector<ClientThread>();
    private ConcurrentMap<Integer, RequestId> pendingClientRequestMap = new ConcurrentHashMap<Integer, RequestId>();

    private Caesar caesar;

    class ClientThread extends Thread {

        int clientId = -1;
        int sequenceNum = 0;
        int localClientId;

        private BlockingQueue<Integer> sends;
        private final Random random;

        public ClientThread(int clientId) {
            this.clientId = clientId;
            this.random = new Random();
        }

        @Override
        public void run() {

            try {

                Thread.sleep(5000);
                // Start the main benchmark execution now
                // TODO:
                while(true) {
                    byte[] bytes = service.createRequest();

                    Request request = new Request(rId, new RequestId(clientId, ++sequenceNum), bytes);
                    RequestId requestId = request.getRequestId();

                    pendingClientRequestMap.put(requestId.getClientId(), requestId);

                    synchronized (requestId) {
                        caesar.propose(request);
                        requestId.wait();
                    }

                    pendingClientRequestMap.remove(requestId.getClientId());
                }

            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        public void execute(int count) throws InterruptedException {
            sends.put(count);
        }

    }

    public ClientManager(int clientCount, int requestsPerClient, int replicaId, Service service) {
        this.service = service;
        this.clientCount = clientCount;
        this.requestsPerClient = requestsPerClient;
        this.replicaId = replicaId;

    }

    public void run() throws IOException, InterruptedException {

        execute(this.clientCount, this.requestsPerClient);
    }

    public void execute(int clientCount, int requests)
            throws IOException, InterruptedException {

        IdGenerator idGenerator = new SimpleIdGenerator(ProcessDescriptor.getInstance().localId * 1000,
                ProcessDescriptor.getInstance().numReplicas);

        //for (int i = clients.size(); i < clientCount; i++) {
        for (int i = 0; i < clientCount; i++) {
            int clientId = idGenerator.next();
//        	System.out.println("ClientId: " + clientId);

            ClientThread client = new ClientThread(clientId, replicaId, i);
            client.start();
            clients.add(client);
        }

        startTime = System.currentTimeMillis();

        // prime the STM and OS-Paxos
        for (int i = 0; i < clientCount; i++) {
            clients.get(i).execute(requests);
        }

        for (int i = 0; i < clientCount; i++) {
            clients.get(i).join();
        }
    }

    public void replyToClient(Request request) {
        if (request != null) {
            RequestId requestId = pendingClientRequestMap.get(request.getRequestId().getClientId());
            if (requestId != null) {
                synchronized (requestId) {
                    requestId.notify();
                    //System.out.print("$");
                }
            }
        }
    }

    Logger logger = LogManager.getLogger(ClientManager.class);
}
