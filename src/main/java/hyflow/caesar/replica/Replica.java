package hyflow.caesar.replica;

import hyflow.caesar.Caesar;
import hyflow.caesar.DecideCallback;
import hyflow.common.Configuration;
import hyflow.common.ProcessDescriptor;
import hyflow.common.Request;
import hyflow.common.RequestId;
import hyflow.main.ClientManager;
import hyflow.service.Service;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class Replica {

    private final static Logger logger = LogManager.getLogger(Replica.class);
    private final BlockingQueue<Request> deliverQueue;
    private final Caesar caesar;
    private final DecideCallback callback;
    private final Service service;
    private ClientManager client;

    public Replica(Configuration config, int localId, Service service) throws IOException {
        ProcessDescriptor.initialize(config, localId);
        this.service = service;
        this.caesar = new Caesar();
        this.deliverQueue = new LinkedBlockingQueue<>();
        callback = new InnerDecideCallback();
    }

    public void start(ClientManager client) throws IOException {
        this.client = client;
        caesar.startCaesar(callback);
    }

    public void submit(Request request) {
        caesar.propose(request);
    }

    private class DeliveryManager implements Runnable {

        @Override
        public void run() {

            while (!Thread.interrupted()) {
                try {
                    Request request = deliverQueue.take();
                    service.executeRequest(request);
                    RequestId rId = request.getId();
                    synchronized (rId) {
                        rId.notifyAll();
                    }
                    client.upCount();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

        }
    }

    private class InnerDecideCallback implements DecideCallback {

        private final DeliveryManager manager;

        public InnerDecideCallback() {
            this.manager = new DeliveryManager();
            new Thread(manager).start();
        }

        @Override
        public void deliver(Request request) {
            try {
                deliverQueue.put(request);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

}
