package hyflow.main;

import hyflow.caesar.Caesar;
import hyflow.common.Request;
import hyflow.common.RequestId;
import hyflow.service.Service;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.layout.SerializedLayout;

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

    private final Service service;
    private final int clientCount;
    private final int requestsPerClient;
    private final int replicaId;

    private Vector<ClientThread> clients = new Vector<ClientThread>();
    private ConcurrentMap<Integer, RequestId> pendingClientRequestMap = new ConcurrentHashMap<Integer, RequestId>();

    private Caesar caesar;

    class ClientThread extends Thread {

        private final int clientId;
        int sequenceNum = 0;

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

                    Request request = new Request(new RequestId(clientId, ++sequenceNum), null, bytes);
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
