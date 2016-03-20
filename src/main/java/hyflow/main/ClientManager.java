package hyflow.main;

import hyflow.caesar.Caesar;
import hyflow.common.IdGenerator;
import hyflow.common.Request;
import hyflow.common.RequestId;
import hyflow.common.SimpleIdGenerator;
import hyflow.service.Service;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by balajiarun on 3/7/16.
 */
public class ClientManager {

    private final Service service;
    private final int clientCount;
    private final int replicaId;
    private final Caesar caesar;
    Logger logger = LogManager.getLogger(ClientManager.class);
    private Vector<ClientThread> clients = new Vector<ClientThread>();
    private ConcurrentMap<Integer, RequestId> pendingClientRequestMap = new ConcurrentHashMap<Integer, RequestId>();

    public ClientManager(int clientCount, int replicaId, Service service, Caesar caesar) throws IOException {
        this.service = service;
        this.clientCount = clientCount;
        this.replicaId = replicaId;
        this.caesar = caesar;
    }

    public void start() {
        //TODO: Change clientCount to numReplicas
        IdGenerator generator = new SimpleIdGenerator(replicaId, clientCount);
        for (int i = 0; i < clientCount; i++) {
            new ClientThread(generator.next()).start();
        }
    }

    public void replyToClient(Request request) {
        if (request != null) {
            RequestId requestId = pendingClientRequestMap.get(request.getRequestId().getClientId());
            if (requestId != null) {
                synchronized (requestId) {
                    requestId.notify();
                }
            }
        }
    }

    class ClientThread extends Thread {

        private final int clientId;
        int sequenceNum = 0;

        public ClientThread(int clientId) {
            this.clientId = clientId;
        }

        @Override
        public void run() {

            try {
                while(true) {
                    Request request = service.createRequest(new RequestId(clientId, sequenceNum++));
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

    }
}
