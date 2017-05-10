package hyflow.caesar;

import hyflow.caesar.messages.FastProposeReply;
import hyflow.common.ProcessDescriptor;
import hyflow.common.Request;
import hyflow.common.RequestId;

import java.util.Arrays;
import java.util.TreeSet;
import java.util.concurrent.ScheduledFuture;

/**
 * Created by balajiarun on 3/14/16.
 */
public class FastProposeReplyInfo {

    private final Request request;
    private final Request specRequest;
    private final FastProposeReply[] replies;

    private final int fastQuorum;
    private final int classicQuorum;

    private final TreeSet<RequestId> predSet;
    private final TreeSet<RequestId> specPredSet;

    private long position;
    private long specPosition;

    private boolean nack;
    private boolean doubt;

    private int count;
    private int specCount;
    private boolean done;

    private ScheduledFuture<?> slowProposeTimer;
    private boolean success = false;
    private boolean specRetryDone = false;


    public FastProposeReplyInfo(Request request, int numReplicas) {
        this.request = request;
        this.specRequest = new Request(request.getId(), request.getObjectIds(), request.getPayload());

        replies = new FastProposeReply[numReplicas];

        predSet = new TreeSet<>();
        specPredSet = new TreeSet<>();

        position = request.getPosition();
        specPosition = request.getPosition();

        nack = false;

        count = 0;
        specCount = 0;

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

        boolean doubtFlag = msg.getStatus() == FastProposeReply.Status.SPECNACK;
        if (!doubtFlag) {

            count++;
            specPredSet.addAll(msg.getPred());
            predSet.addAll(msg.getPred());
            if (!nack)
                nack = (msg.getStatus() == FastProposeReply.Status.NACK);
            position = Math.max(msg.position(), position);

        } else {

            specCount++;
            doubt = true;
            specPredSet.addAll(msg.getPred());
            specPosition = Math.max(msg.position(), specPosition);

        }
    }

    public Request updateAndGetSpecRequest() {
        specRequest.setPred(specPredSet);
        specRequest.setPosition(specPosition);

        return specRequest;
    }

    public boolean isFastQuorum() {
        return (count >= fastQuorum);
    }

    public boolean isClassicQuorum() {
        return (count >= classicQuorum);
    }

    public boolean isClassicQuorumWithSpec() {
        return (count + specCount >= classicQuorum);
    }

    public boolean hasNack() {
        return nack;
    }

    public boolean hasSpecNack() {
        return doubt;
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

    public FastProposeReply[] getReplies() {
        return replies;
    }

    public long getSpecPosition() {
        return specPosition;
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess() {
        success = true;
    }

    public boolean isSpecRetryDone() {
        return specRetryDone;
    }

    public void setSpecRetryDone() {
        specRetryDone = true;
    }
}
