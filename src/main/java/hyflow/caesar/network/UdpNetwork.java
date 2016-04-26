package hyflow.caesar.network;

import hyflow.caesar.messages.Message;
import hyflow.caesar.messages.MessageFactory;
import hyflow.common.Configuration;
import hyflow.common.KillOnExceptionHandler;
import hyflow.common.PID;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Represents network based on UDP. Provides basic methods, sending messages to
 * other replicas and receiving messages. This class didn't provide any
 * guarantee that sent message will be received by target. It is possible that
 * some messages will be lost.
 *
 */
public class UdpNetwork extends Network {
    private final static Logger logger = Logger.getLogger(UdpNetwork.class.getCanonicalName());
    private final DatagramSocket datagramSocket;
    private final Thread readThread;
    private final SocketAddress[] addresses;
    private boolean started = false;

    /**
     * @throws SocketException
     */
    public UdpNetwork() throws SocketException {
        addresses = new SocketAddress[p.numReplicas];
        for (int i = 0; i < addresses.length; i++) {
            PID pid = p.config.getProcess(i);
            addresses[i] = new InetSocketAddress(pid.getHostname(), pid.getReplicaPort());
        }

        int localPort = p.getLocalProcess().getReplicaPort();
        logger.info("Opening port: " + localPort);
        datagramSocket = new DatagramSocket(localPort);

        datagramSocket.setReceiveBufferSize(Configuration.UDP_RECEIVE_BUFFER_SIZE);
        datagramSocket.setSendBufferSize(Configuration.UDP_SEND_BUFFER_SIZE);

        readThread = new Thread(new SocketReader(), "UdpReader");
        readThread.setUncaughtExceptionHandler(new KillOnExceptionHandler());
    }

    @Override
    public void start() {
        if (!started) {
            readThread.start();
            started = true;
        }
    }

    /**
     * Blocks until there is space in the OS to buffer the message. Normally it
     * should return immediately. Specified byte array should be serialized
     * message (without any header like id of replica).
     * <p>
     * The sentMessage event in listeners is not fired after calling this
     * method.
     *
     * @param message - the message to send
     * @param destinations - the id's of replicas to send message to
     * @throws IOException if an I/O error occurs
     */
    void send(byte[] message, BitSet destinations) {
        // prepare packet to send
        byte[] data = new byte[message.length + 4];
        ByteBuffer.wrap(data).putInt(p.localId).put(message);
        DatagramPacket dp = new DatagramPacket(data, data.length);

        for (int i = destinations.nextSetBit(0); i >= 0; i = destinations.nextSetBit(i + 1)) {
            dp.setSocketAddress(addresses[i]);
            try {
                datagramSocket.send(dp);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void sendMessage(Message message, BitSet destinations) {
        assert message != null && !destinations.isEmpty() : "Null message or no destinations";
        message.setSentTime();
        byte[] messageBytes = message.toByteArray();
        // if (messageBytes.length > Config.MAX_UDP_PACKET_SIZE + 4)
        if (messageBytes.length > p.maxUdpPacketSize + 4) {
            throw new RuntimeException("Data packet too big. Size: " +
                    messageBytes.length + ", limit: " + p.maxUdpPacketSize +
                    ". Packet not sent.");
        }

        send(messageBytes, destinations);
    }

    @Override
    public boolean send(byte[] message, int destination) {
        byte[] data = new byte[message.length + 4];
        ByteBuffer.wrap(data).putInt(p.localId).put(message);
        DatagramPacket dp = new DatagramPacket(data, data.length);

        dp.setSocketAddress(addresses[destination]);
        try {
            datagramSocket.send(dp);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return true;
    }

    /**
     * Reads messages from the network and enqueues them to be handled by the
     * dispatcher thread.
     */
    private class SocketReader implements Runnable {
        public void run() {
            logger.info(Thread.currentThread().getName() +
                    " thread started. Waiting for UDP messages");
            try {
                while (!Thread.interrupted()) {
                    // byte[] buffer = new byte[Config.MAX_UDP_PACKET_SIZE + 4];
                    byte[] buffer = new byte[p.maxUdpPacketSize + 4];
                    // Read message and enqueue it for processing.
                    DatagramPacket dp = new DatagramPacket(buffer, buffer.length);
                    datagramSocket.receive(dp);

                    ByteArrayInputStream bais = new ByteArrayInputStream(dp.getData(),
                            dp.getOffset(), dp.getLength());
                    DataInputStream dis = new DataInputStream(bais);

                    int sender = dis.readInt();
                    byte[] data = new byte[dp.getLength() - 4];
                    dis.read(data);

                    try {
                        Message message = MessageFactory.readByteArray(data);
                        if (logger.isLoggable(Level.FINE)) {
                            logger.fine("Received from " + sender + ":" + message);
                        }
                        fireReceiveMessage(message, sender);
                    } catch (ClassNotFoundException e) {
                        logger.log(Level.WARNING, "Error deserializing message", e);
                    }
                }
            } catch (IOException e) {
                logger.log(Level.SEVERE, "Fatal error.", e);
            }
        }
    }
}