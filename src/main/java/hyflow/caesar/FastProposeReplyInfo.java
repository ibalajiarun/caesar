package hyflow.caesar;

import hyflow.caesar.messages.FastProposeReply;
import hyflow.common.ProcessDescriptor;
import hyflow.common.Request;
import hyflow.common.RequestId;

import java.util.Arrays;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ScheduledFuture;

/**
 * Created by balajiarun on 3/14/16.
 */
public class FastProposeReplyInfo {

    private final Request request;
    private final FastProposeReply[] replies;

    private final int fastQuorum;
    private final int classicQuorum;

    private final TreeSet<RequestId> predSet;
    private long position;
    private boolean nack;

    private int count;
    private boolean done;

    private ScheduledFuture<?> slowProposeTimer;

    public FastProposeReplyInfo(Request request, int numReplicas) {
        this.request = request;

        replies = new FastProposeReply[numReplicas];

        predSet = new TreeSet<>();
        position = request.getPosition();
        nack = false;

        count = 0;
        done = false;

        classicQuorum = ProcessDescriptor.getInstance().classicQuorum;
        fastQuorum = ProcessDescriptor.getInstance().fastQuorum;
//        classicQuorum = numReplicas;
//        fastQuorum = numReplicas;
    }

    public Request updateAndGetRequest() {
        request.setPred(predSet);
        request.setPosition(position);
        return request;
    }

    public boolean isDone() {
        return done;
    }

    public void setDone() {
        done = true;
    }

    public void addReply(FastProposeReply msg, int sender) {
        replies[sender] = msg;
        count++;
        predSet.addAll(msg.getPred());
        if (!nack)
            nack = (msg.getStatus() == FastProposeReply.Status.NACK);
        position = Math.max(msg.position(), position);
    }

    public boolean isFastQuorum() {
        return (count >= fastQuorum);
    }

    public boolean isClassicQuorum() {
        return (count >= classicQuorum);
    }

    public boolean hasNack() {
        return nack;
    }

    public long getMaxPosition() {
        return position;
    }

    public ScheduledFuture<?> getSlowProposeFuture() {
        return slowProposeTimer;
    }

    public void setSlowProposeFuture(ScheduledFuture<?> slowProposeTimer) {
        this.slowProposeTimer = slowProposeTimer;
    }

    @Override
    public String toString() {
        return "FastProposeReplyInfo{" +
                "request=" + request +
                ", replies=" + Arrays.toString(replies) +
                ", fastQuorum=" + fastQuorum +
                ", classicQuorum=" + classicQuorum +
                ", predSet=" + predSet +
                ", position=" + position +
                ", nack=" + nack +
                ", count=" + count +
                ", done=" + done +
                '}';
    }
}
