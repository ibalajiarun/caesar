package hyflow.caesar;

import hyflow.common.ProcessDescriptor;

/**
 * Created by balajiarun on 3/11/16.
 */
public class TimestampGenerator {

    private static TimestampGenerator instance = new TimestampGenerator();
    private long timestamp;
    private int localId;
    private int numReplicas;

    public static TimestampGenerator getInstance() {
        return instance;
    }

    private TimestampGenerator() {
        localId = ProcessDescriptor.getInstance().localId;
        numReplicas = ProcessDescriptor.getInstance().numReplicas;
        timestamp = -1;
    }

    public synchronized long getTimeStamp() {
        do {
            timestamp++;
        } while (timestamp % numReplicas != localId);
        return timestamp;
    }

    public synchronized void setTimestamp(long observed) {
        if(observed <= timestamp)
            return;

        timestamp = observed;
    }

}
