package hyflow.caesar.replica;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import hyflow.common.Configuration;
import hyflow.common.ProcessDescriptor;
import hyflow.common.SingleThreadDispatcher;
import hyflow.caesar.Caesar;
import hyflow.service.Service;

/**
 * Manages replication of a service. Receives requests from the client, orders
 * them using Paxos, executes the ordered requests and sends the reply back to
 * the client.
 * <p>
 * Example of usage:
 * <p>
 * <blockquote>
 * 
 * <pre>
 * public static void main(String[] args) throws IOException {
 *  int localId = Integer.parseInt(args[0]);
 *  Replica replica = new Replica(localId, new MapService());
 *  replica.start();
 * }
 * </pre>
 * 
 * </blockquote>
 */
public class Replica {
    /**
     * Represents different crash models.
     */
    public enum CrashModel {
        /**
         * Synchronous writes to disk are made on critical path of caesar
         * execution. Catastrophic failures will be handled correctly. It is
         * slower than other algorithms.
         */
        FullStableStorage,
        CrashStop,
        EpochSS,
        ViewSS
    }

    private String logPath;

    private Caesar caesar;

    private static final boolean LOG_DECISIONS = false;

    /** Next request to be executed. */
    private int executeUB = 0;

    /**
     * For each client, keeps the sequence id of the last request executed from
     * the client.
     * 
     * TODO: the executedRequests map grows and is NEVER cleared!
     * 
     * For theoretical correctness, it must stay so. In practical approach, give
     * me unbounded storage, limit the overall client count or simply let eat
     * some stale client requests.
     * 
     * Bad solution keeping correctness: record time stamp from client, his
     * request will only be valid for 5 minutes, after that time - go away. If
     * client resends it after 5 minutes, we ignore request. If client changes
     * the time stamp, it's already a new request, right? Client with broken
     * clocks will have bad luck.
     * 
     * This is accessed by the Selector threads, so it must be thread-safe
     */
    private final SingleThreadDispatcher dispatcher;
    private final ProcessDescriptor descriptor;

    private final Configuration config;


    /**
     * Initializes new instance of <code>Replica</code> class.
     * <p>
     * This constructor doesn't start the replica and Paxos protocol. In order
     * to run it the {@link #start()} method should be called.
     * 
     * @param config - the configuration of the replica
     * @param localId - the id of replica to create
     * @param service - the state machine to execute request on
     * @throws IOException if an I/O error occurs
     */
    public Replica(Configuration config, int localId, Service service) throws IOException {
        this.dispatcher = new SingleThreadDispatcher("Replica");
        this.config = config;

        ProcessDescriptor.initialize(config, localId);
        descriptor = ProcessDescriptor.getInstance();

        logPath = descriptor.logPath + '/' + localId;

    }

    /**
     * Starts the replica.
     * <p>
     * First the recovery phase is started and after that the replica joins the
     * Paxos protocol and starts the client manager and the underlying service.
     * 
     * @throws IOException if some I/O error occurs
     */
    public void start() throws IOException {
        logger.info("Recovery phase started.");

//        dispatcher.start();
    }

    public void forceExit() {
        dispatcher.shutdownNow();
    }

    /**
     * Sets the path to directory where all logs will be saved.
     * 
     * @param path to directory where logs will be saved
     */
    public void setLogPath(String path) {
        logPath = path;
    }

    /**
     * Gets the path to directory where all logs will be saved.
     * 
     * @return path
     */
    public String getLogPath() {
        return logPath;
    }

    public Configuration getConfiguration() {
        return config;
    }

    public SingleThreadDispatcher getReplicaDispatcher() {
        return dispatcher;
    }

    private final static Logger logger = Logger.getLogger(Replica.class.getCanonicalName());



}
