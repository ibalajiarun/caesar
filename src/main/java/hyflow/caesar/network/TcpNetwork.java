package hyflow.caesar.network;

import hyflow.caesar.messages.Message;
import hyflow.common.KillOnExceptionHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.BitSet;

public class TcpNetwork extends Network implements Runnable {
    private final static Logger logger = LogManager.getLogger(TcpNetwork.class);
    private final TcpConnection[] connections;
    private final ServerSocket server;
    private final Thread acceptorThread;
    private final int id;
    private boolean started = false;

    /**
     * Creates new network for handling connections with other replicas.
     *
     * @throws IOException if opening server socket fails
     */
    public TcpNetwork(int id) throws IOException {
        this.id = id;
        this.connections = new TcpConnection[p.numReplicas];

        int port = p.getLocalProcess().getReplicaPort() + (id * 100);
        logger.info("Opening port: " + port);
        this.server = new ServerSocket();
//        server.setReceiveBufferSize(TcpConnection.TCP_BUFFER_SIZE);
        server.bind(new InetSocketAddress((InetAddress) null, port));
        logger.info("Buffer Size:" + server.getReceiveBufferSize());
        this.acceptorThread = new Thread(this, "TcpNetwork");
        acceptorThread.setUncaughtExceptionHandler(new KillOnExceptionHandler());
    }

    @Override
    public void start() {
        if (!started) {
            for (int i = 0; i < connections.length; i++) {
                if (i < p.localId) {
                    connections[i] = new TcpConnection(this, p.config.getProcess(i), id, false);
                    connections[i].start();
                }
                if (i > p.localId) {
                    connections[i] = new TcpConnection(this, p.config.getProcess(i), id, true);
                    connections[i].start();
                }
            }
            // Start the thread that listens and accepts new connections.
            // Must be started after the connections are initialized (code
            // above)
            acceptorThread.start();
            started = true;
        }
    }

    /**
     * Sends binary data to specified destination.
     *
     * @param message - binary data to send
     * @param destination - id of replica to send data to
     * @return true if message was sent; false if some error occurred
     */
    public boolean send(byte[] message, int destination) {
        assert destination != p.localId;
        return connections[destination].send(message);
    }

    @Override
    public void sendMessage(Message message, BitSet destinations) {
        assert !destinations.isEmpty() : "Sending a message to no one";

        byte[] bytes = message.toByteArray();
        for (int i = destinations.nextSetBit(0); i >= 0; i = destinations.nextSetBit(i + 1)) {
            if (i == p.localId) {
                // do not send message to self (just fire event)
                fireReceiveMessage(message, p.localId);
            } else {
                send(bytes, i);
            }
        }

        // Not really sent, only queued for sending,
        // but it's good enough for the notification
        fireSentMessage(message, destinations);
    }

    /**
     * Main loop which accepts incoming connections.
     */
    public void run() {
        logger.info(Thread.currentThread().getName() + " thread started");
        while (!Thread.interrupted()) {
            try {
                Socket socket = server.accept();
                initializeConnection(socket);
            } catch (IOException e) {
                // TODO: probably too many open files exception occurred;
                // should we open server socket again or just wait and ignore
                // this exception?
                throw new RuntimeException(e);
            }
        }
    }

    private void initializeConnection(Socket socket) {
        try {
            logger.info("Received connection from " + socket.getRemoteSocketAddress());
//            socket.setReceiveBufferSize(TcpConnection.TCP_BUFFER_SIZE);
//            socket.setSendBufferSize(TcpConnection.TCP_BUFFER_SIZE);
            socket.setTcpNoDelay(true);
            logger.info("Passive. RcvdBuffer: " + socket.getReceiveBufferSize() +
                    ", SendBuffer: " + socket.getSendBufferSize());
            DataInputStream input = new DataInputStream(
                    new BufferedInputStream(socket.getInputStream()));
            DataOutputStream output = new DataOutputStream(
                    new BufferedOutputStream(socket.getOutputStream()));
            int replicaId = input.readInt();

            if (replicaId < 0 || replicaId >= p.numReplicas) {
                logger.warn("Remoce host id is out of range: " + replicaId);
                socket.close();
                return;
            }
            if (replicaId == p.localId) {
                logger.warn("Remote replica has same id as local: " + replicaId);
                socket.close();
                return;
            }

            connections[replicaId].setConnection(socket, input, output);
        } catch (IOException e) {
            logger.warn("Initialization of accepted connection failed.", e);
            try {
                socket.close();
            } catch (IOException e1) {
            }
        }
    }

    public void closeAll() {
        for (TcpConnection c : connections) {
            try {
                if (c != null) c.stop();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public boolean isStarted() {
        return started;
    }
}
