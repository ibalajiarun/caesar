package hyflow.common;

/**
 * Contains all the information describing the local process, including the
 * local id and the configuration of the system.
 * 
 * @author Nuno Santos (LSR)
 * @author Balaji Arun (VT)
 */
public final class ProcessDescriptor {

    /*---------------------------------------------
     * The following properties are read from the
     * paxos.properties file
     *---------------------------------------------*/
    /**
     * Maximum UDP packet size in java is 65507. Higher than that and the send
     * method throws an exception.
     *
     * In practice, most networks have a lower limit on the maximum packet size
     * they can transmit. If this limit is exceeded, the lower layers will
     * usually fragment the packet, but in some cases there's a limit over which
     * large packets are simply dropped or raise an error.
     *
     * A safe value is the maximum ethernet frame: 1500 - maximum Ethernet
     * payload 20/40 - ipv4/6 header 8 - UDP header.
     *
     * Usually values up to 8KB are safe.
     */
    public static final String MAX_UDP_PACKET_SIZE = "MaxUDPPacketSize";
    public static final int DEFAULT_MAX_UDP_PACKET_SIZE = 8 * 1024;
    /**
     * Protocol to use between replicas. TCP, UDP or Generic, which combines
     * both
     */
    public static final String NETWORK = "Network";
    public static final String DEFAULT_NETWORK = "TCP";
    /**
     * The maximum size of batched request.
     */
    public static final String BATCH_SIZE = "BatchSize";
    public static final int DEFAULT_BATCH_SIZE = 65507;
    /** How long to wait until suspecting the leader. In milliseconds */
    public static final String FD_SUSPECT_TO = "FDSuspectTimeout";
    public static final int DEFAULT_FD_SUSPECT_TO = 1000;
    /** Interval between sending heartbeats. In milliseconds */
    public final static String FD_SEND_TO = "FDSendTimeout";
    public static final int DEFAULT_FD_SEND_TO = 500;

    /**
     * Maximum time in ms that a batch can be delayed before being proposed.
     * Used to aggregate several requests on a single proposal, for greater
     * efficiency. (Naggle's algorithm for state machine replication).
     */
    public static final String MAX_BATCH_DELAY = "MaxBatchDelay";
    public static final int DEFAULT_MAX_BATCH_DELAY = 10;

    /**
     * Before any snapshot was made, we need to have an estimate of snapshot
     * size. Value given as for now is 1 KB
     */
    public static final String RETRANSMIT_TIMEOUT = "RetransmitTimeoutMilisecs";
    public static final long DEFAULT_RETRANSMIT_TIMEOUT = 1000;

    /** If a TCP connection fails, how much to wait for another try */
    public static final String TCP_RECONNECT_TIMEOUT = "TcpReconnectMilisecs";
    public static final long DEFAULT_TCP_RECONNECT_TIMEOUT = 1000;

    private static final String PROPOSER_MAP_SIZE = "ProposerMapSize";
    private static final int DEFAULT_PROPOSER_MAP_SIZE = 100000;

    private static final String PROPOSER_SLEEP = "ProposerSleep";
    private static final int DEFAULT_PROPOSER_SLEEP = 1;

    private static final String CREQ_THREADS = "CReqThreads";
    private static final int DEFAULT_CREQ_THREADS = 1;

    private static final String PROPOSAL_THREADS = "ProposalThreads";
    private static final int DEFAULT_PROPOSAL_THREADS = 1;

    private static final String AUX_THREADS = "AuxThreads";
    private static final int DEFAULT_AUX_THREADS = 1;

    private static final String INT_THREADS = "IntThreads";
    private static final int DEFAULT_INT_THREADS = 1;

    private static final String STABLE_THREADS = "StableThreads";
    private static final int DEFAULT_STABLE_THREADS = 1;

    private static final String DELIVERY_THREADS = "DeliveryThreads";
    private static final int DEFAULT_DELIVERY_THREADS = 1;

    private static final String ZMQ_HOST = "ZmqHost";
    private static final String DEFAULT_ZMQ_HOST = "localhost";

    private static final String ZMQ_PORT = "ZmqPort";
    private static final String DEFAULT_ZMQ_PORT = "5000";

    private static final String RECOVERY_LEADER = "RecoveryLeader";
    private static final int DEFAULT_RECOVERY_LEADER = 0;

    private static final String FP_TIMEOUT = "FPTimeout";
    private static final int DEFAULT_FP_TIMEOUT = 100;

    private static final String MONITOR_INTERVAL = "MonitorInterval";
    private static final int DEFAULT_MONITOR_INTERVAL = 2000;

    /*
     * Singleton class with static access. This allows any class on the JVM to
     * statically access the process descriptor without needing to be given a
     * reference.
     */
    private static ProcessDescriptor instance;
    public final Configuration config;
    /*
     * Exposing fields is generally not good practice, but here they are made
     * final, so there is no danger of exposing them. Advantage: less
     * boilerplate code.
     */
    public final int localId;
    public final int numReplicas;

    public final int fastQuorum;
    public final int classicQuorum;

    public final int maxUdpPacketSize;

    public final String network;

    public final long tcpReconnectTimeout;
    public final int fdSuspectTimeout;
    public final int fdSendTimeout;

    public final int proposerMapSize;

    public final int proposerSleep;

    public final int cReqThreads;
    public final int proposalThreads;
    public final int auxThreads;
    public final int intThreads;
    public final int stableThreads;
    public final int deliveryThreads;

    public final String zmqHost;
    public final String zmqPort;

    public final int recoveryLeader;
    public final int fpTimeout;
    public final int monitorInterval;

    private ProcessDescriptor(Configuration config, int localId) {
        this.localId = localId;
        this.config = config;

        this.numReplicas = config.getN();

        this.fastQuorum = this.numReplicas - this.numReplicas / 4;
        this.classicQuorum = this.numReplicas / 2 + 1;

        this.proposerSleep = config.getIntProperty(PROPOSER_SLEEP, DEFAULT_PROPOSER_SLEEP);

        this.cReqThreads = config.getIntProperty(CREQ_THREADS, DEFAULT_AUX_THREADS);
        this.proposalThreads = config.getIntProperty(PROPOSAL_THREADS, DEFAULT_PROPOSAL_THREADS);
        this.auxThreads = config.getIntProperty(AUX_THREADS, DEFAULT_AUX_THREADS);
        this.intThreads = config.getIntProperty(INT_THREADS, DEFAULT_AUX_THREADS);
        this.stableThreads = config.getIntProperty(STABLE_THREADS, DEFAULT_STABLE_THREADS);
        this.deliveryThreads = config.getIntProperty(DELIVERY_THREADS, DEFAULT_DELIVERY_THREADS);

        this.proposerMapSize = config.getIntProperty(PROPOSER_MAP_SIZE, DEFAULT_PROPOSER_MAP_SIZE);

        this.zmqHost = config.getProperty(ZMQ_HOST, DEFAULT_ZMQ_HOST);
        this.zmqPort = config.getProperty(ZMQ_PORT, DEFAULT_ZMQ_PORT);

//        this.batchingLevel = config.getIntProperty(BATCH_SIZE, DEFAULT_BATCH_SIZE);
        this.maxUdpPacketSize = config.getIntProperty(MAX_UDP_PACKET_SIZE,
                DEFAULT_MAX_UDP_PACKET_SIZE);
//        this.maxBatchDelay = config.getIntProperty(MAX_BATCH_DELAY,
//                DEFAULT_MAX_BATCH_DELAY);
        this.network = config.getProperty(NETWORK, DEFAULT_NETWORK);

//        this.retransmitTimeout = config.getLongProperty(RETRANSMIT_TIMEOUT,
//                DEFAULT_RETRANSMIT_TIMEOUT);
        this.tcpReconnectTimeout = config.getLongProperty(TCP_RECONNECT_TIMEOUT,
                DEFAULT_TCP_RECONNECT_TIMEOUT);

        this.fdSuspectTimeout = config.getIntProperty(FD_SUSPECT_TO,
                DEFAULT_FD_SUSPECT_TO);
        this.fdSendTimeout = config.getIntProperty(FD_SEND_TO,
                DEFAULT_FD_SEND_TO);

        this.recoveryLeader = config.getIntProperty(RECOVERY_LEADER,
                DEFAULT_RECOVERY_LEADER);

        this.fpTimeout = config.getIntProperty(FP_TIMEOUT,
                DEFAULT_FP_TIMEOUT);

        this.monitorInterval = config.getIntProperty(MONITOR_INTERVAL,
                DEFAULT_MONITOR_INTERVAL);
    }

    public static void initialize(Configuration config, int localId) {
        ProcessDescriptor.instance = new ProcessDescriptor(config, localId);
    }

    public static ProcessDescriptor getInstance() {
        return instance;
    }
    
    /**
     *
     * @return the local process
     */
    public PID getLocalProcess() {
        return config.getProcess(localId);
    }

    public PID getProcess(int id) {
        return config.getProcess(id);
    }

    public boolean isLocalProcessLeader() {
        return recoveryLeader == localId;
    }
}
