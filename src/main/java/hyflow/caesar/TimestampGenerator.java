package hyflow.caesar;

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
        timestamp = localId;
    }

    public synchronized long newTimestamp() {
        long retVal = timestamp;
        timestamp += numReplicas;
        return retVal;
    }

    public synchronized void setTimestamp(long observed) {
        if(observed <= timestamp)
            return;
        timestamp = observed + numReplicas + localId - observed % numReplicas;
        assert timestamp % numReplicas == localId;
    }

}
