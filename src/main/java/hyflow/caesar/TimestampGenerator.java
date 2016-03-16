package hyflow.caesar;

import hyflow.common.ProcessDescriptor;

/**
 * Created by balajiarun on 3/11/16.
 */
public class TimestampGenerator {

    private long timestamp;
    private int localId;
    private int numReplicas;

    public TimestampGenerator(int localId, int numReplicas) {
        this.localId = localId;
        this.numReplicas = numReplicas;
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
