package hyflow.caesar.network;

import hyflow.caesar.messages.Message;
import hyflow.caesar.messages.MessageFactory;
import hyflow.common.KillOnExceptionHandler;
import hyflow.common.PID;
import hyflow.common.ProcessDescriptor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * This class is responsible for handling stable TCP connection to other
 * replica, provides two methods for establishing new connection: active and
 * passive. In active mode we try to connect to other side creating new socket
 * and connects. If passive mode is enabled, then we wait for socket from the
 * <code>SocketServer</code> provided by <code>TcpNetwork</code>.
 * <p>
 * Every time new message is received from this connection, it is deserialized,
 * and then all registered network listeners in related <code>TcpNetwork</code>
 * are notified about it.
 *
 * @see TcpNetwork
 */
public class TcpConnection {
    //    public static final int TCP_BUFFER_SIZE = 4* 1024 * 1024;
    private final static Logger logger = LogManager.getLogger(TcpConnection.class.getCanonicalName());
    private final PID replica;
    /** true if connection should be started by this replica; */
    private final boolean active;
    private final int id;
    private final TcpNetwork network;
    private final Thread senderThread;
    private final Thread receiverThread;
    private final BlockingQueue<byte[]> sendQueue = new ArrayBlockingQueue<byte[]>(200000);
    private Socket socket;
    private DataInputStream input;
    private OutputStream output;
    private volatile boolean connected = false;
    private int dropped = 0;
    private int droppedFull = 0;

    /**
     * Creates a new TCP connection to specified replica.
     *
     * @param network - related <code>TcpNetwork</code>.
     * @param replica - replica to connect to.
     * @param active - initiates connection if true; waits for remote connection
     *            otherwise.
     */
    public TcpConnection(TcpNetwork network, PID replica, int id, boolean active) {
        this.network = network;
        this.replica = replica;
        this.id = id;
        this.active = active;

        logger.info("Creating connection: " + replica + " - " + active);

        this.receiverThread = new Thread(new ReceiverThread(), "ReplicaIORcv-" + this.replica.getId());
        this.senderThread = new Thread(new Sender(), "ReplicaIOSnd-" + this.replica.getId());
        receiverThread.setUncaughtExceptionHandler(new KillOnExceptionHandler());
        senderThread.setUncaughtExceptionHandler(new KillOnExceptionHandler());
    }

    /**
     * Starts the receiver and sender thread.
     */
    public synchronized void start() {
        receiverThread.start();
        senderThread.start();
    }

    /**
     * Sends specified binary packet using underlying TCP connection.
     *
     * @param message - binary packet to send
     * @return true if sending message was successful
     */
    public boolean send(byte[] message) {
        try {
            //            boolean queueFull = false;
            //            if (sendQueue.remainingCapacity() < 2) {
            //                logger.warning("Send queue remaining: " + sendQueue.remainingCapacity() + " to replica: " + senderThread.getName());
            //                queueFull = true;
            //            }
            //            sendQueue.put(message);
            if (connected)  {
                //            boolean enqueued = sendQueue.offer(message);
                long start = System.currentTimeMillis();
                sendQueue.put(message);
                int delta = (int) (System.currentTimeMillis() - start);
                if (delta > 10) {
                    logger.warn("Wait time: " + delta);
                }
//                boolean enqueued = sendQueue.offer(message, 10, TimeUnit.MILLISECONDS);
//                if (!enqueued) {
//                    if (droppedFull % 16 == 0) {
//                        logger.warning("Dropping message, send queue full. To: " + replica.getId() + ". " + droppedFull);
//                    }
//                    droppedFull++;
//                }
            } else {
                if (dropped % 1024 == 0) {
                    logger.fatal("Dropping message, not connected. To: " + replica.getId() + ". " + dropped);
                }
                dropped++;
            }
            //            sendQueue.put((message);
            //            if (queueFull) {
            //                logger.warning("Enqueued");
            //            }
        } catch (InterruptedException e) {
            logger.warn("Thread interrupted. Terminating.");
            Thread.currentThread().interrupt();
        }
        return true;
    }

    /**
     * Registers new socket to this TCP connection. Specified socket should be
     * initialized connection with other replica. First method tries to close
     * old connection and then set-up new one.
     *
     * @param socket - active socket connection
     * @param input - input stream from this socket
     * @param output - output stream from this socket
     */
    public synchronized void setConnection(Socket socket, DataInputStream input,
                                           DataOutputStream output) {
        assert socket.isConnected() : "Invalid socket state";

        // first close old connection
        close();

        // initialize new connection
        this.socket = socket;
        this.input = input;
        this.output = output;
        connected = true;

        // if main thread wait for this connection notifyClient it
        notifyAll();
    }

    /**
     * Stops current connection and stops all underlying threads.
     *
     * Note: This method waits until all threads are finished.
     *
     * @throws InterruptedException
     */
    public void stop() throws InterruptedException {
        close();
        receiverThread.interrupt();
        senderThread.interrupt();

        receiverThread.join();
        senderThread.join();
    }

    /**
     * Establishes connection to host specified by this object. If this is
     * active connection then it will try to connect to other side. Otherwise we
     * will wait until connection will be set-up using
     * <code>setConnection</code> method. This method will return only if the
     * connection is established and initialized properly.
     *
     * @throws InterruptedException
     */
    private void connect() throws InterruptedException {
        if (active) {
            // this is active connection so we try to connect to host
            while (true) {
                try {
                    socket = new Socket();
//                    socket.setReceiveBufferSize(TCP_BUFFER_SIZE);
//                    socket.setSendBufferSize(TCP_BUFFER_SIZE);
                    logger.warn("RcvdBuffer: " + socket.getReceiveBufferSize() +
                            ", SendBuffer: " + socket.getSendBufferSize());
                    socket.setTcpNoDelay(true);

                    logger.info("Connecting to: " + replica);
                    try {
                        socket.connect(new InetSocketAddress(replica.getHostname(),
                                replica.getReplicaPort() + id * 100));
                    } catch (ConnectException e) {
                        logger.warn("TCP connection with replica " + replica.getId() + " failed");
                        Thread.sleep(ProcessDescriptor.getInstance().tcpReconnectTimeout);
                        continue;
                    }

                    input = new DataInputStream(
                            new BufferedInputStream(socket.getInputStream()));
//                    output = new DataOutputStream(
//                            new BufferedOutputStream(socket.getOutputStream()));
//                    output.writeInt(ProcessDescriptor.getInstance().localId);

                    output = socket.getOutputStream();
                    int v = ProcessDescriptor.getInstance().localId;
                    output.write((v >>> 24) & 0xFF);
                    output.write((v >>> 16) & 0xFF);
                    output.write((v >>>  8) & 0xFF);
                    output.write(v & 0xFF);
                    output.flush();
                    // connection established
                    break;
                } catch (IOException e) {
                    // some other problem (possibly other side closes
                    // connection while initializing connection); for debug
                    // purpose we print this message
                    long sleepTime = ProcessDescriptor.getInstance().tcpReconnectTimeout;
                    logger.warn("Error connecting to " + replica + ". Reconnecting in " + sleepTime, e);
                    Thread.sleep(sleepTime);
                }
            }

            // Wake up the sender thread
            synchronized (this) {
                connected = true;
                notifyAll();
            }

        } else {
            // this is passive connection so we are waiting until other replica
            // connect to us; we will be notified by setConnection method
            synchronized (this) {
                while (!connected) {
                    wait();
                }
            }
        }
    }

    /**
     * Closes the connection.
     */
    private synchronized void close() {
        connected = false;
        if (socket != null && socket.isConnected()) {
            logger.info("Closing socket ...");
            try {
                socket.shutdownOutput();

                // TODO not clean socket closing; we have to wait until all data
                // will be received from server; after closing output stream we
                // should wait until we read all data from input stream;
                // otherwise RST will be send
                socket.close();
                socket = null;
                logger.info("Socket closed.");
            } catch (IOException e) {
                logger.warn("Error closing socket: " + e.getMessage());
            }
        }
    }

    final class Sender implements Runnable {
        public void run() {
            logger.info("Sender thread started.");
            try {
                int count = 0;
                while (!Thread.interrupted()) {
//                    if (logger.isLoggable(Level.FINE)) {
//                    if (sendQueue.size() > 64) {
//                        logger.warning("Queue size: " + sendQueue.size());
//                    }
//                    }
                    byte[] msg = sendQueue.take();
                    // ignore message if not connected
                    // Works without memory barrier because connected is volatile
                    if (!connected) {
                        continue;
                    }

                    try {
                        output.write(msg);
//                        count++;
//                        if(count % 100 == 0) {
                        output.flush();
//                        }
                    } catch (IOException e) {
                        logger.warn("Error sending message", e);
                        close();
                    }
                }
            } catch (InterruptedException e) {
                logger.fatal("Sender thread has been interupted and stopped.");
            }
        }
    }

    /**
     * Main loop used to connect and read from the socket.
     */
    final class ReceiverThread implements Runnable {
        public void run() {
            while (true) {
                // wait until connection is established
                logger.warn("Waiting for tcp connection to " + replica.getId());

                try {
                    connect();
                } catch (InterruptedException e) {
                    logger.fatal("Receiver thread has been interupted.");
                    break;
                }
                logger.info("Tcp connected " + replica.getId());

                long start, time, sumTime = 0, maxTime = 0;
                double count = 0;
                while (true) {
                    if (Thread.interrupted()) {
                        logger.fatal("Receiver thread has been interrupted.");
                        close();
                        return;
                    }

                    try {
                        Message message = MessageFactory.create(input);
                        if (logger.isTraceEnabled()) {
                            logger.trace("Received [" + replica.getId() + "] " + message +
                                    " size: " + message.byteSize());
                        }
//                        start = System.currentTimeMillis();

                        network.fireReceiveMessage(message, replica.getId());

//                        time = System.currentTimeMillis() - start;
//                        sumTime += time;
//                        maxTime = Math.max(sumTime, maxTime);
//                        count++;
//                        if(count % 5000 == 0) {
//                            System.out.println("NETWORK: MAX: " + maxTime + "AVG: " + sumTime/count);
//                            sumTime = 0;
//                            count = 0;
//                        }
                    } catch (Exception e) {
                        // end of stream or problem with socket occurred so
                        // close connection and try to establish it again
                        logger.fatal("Error reading message", e);
                        close();
                        break;
                    }
                }
            }
        }
    }
}
