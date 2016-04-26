package hyflow.caesar;

import hyflow.caesar.messages.SlowProposeReply;
import hyflow.common.ProcessDescriptor;
import hyflow.common.Request;
import hyflow.common.RequestId;

import java.util.SortedSet;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * Created by balajiarun on 3/14/16.
 */
public class SlowProposeReplyInfo {

    private final Request request;
    private final SlowProposeReply[] replies;

    private final int classicQuorum;

    private final SortedSet<RequestId> predSet;
    private long position;
    private boolean nack;

    private int count;
    private boolean done;

    public SlowProposeReplyInfo(Request request, int numReplicas) {
        this.request = request;

        replies = new SlowProposeReply[numReplicas];

        predSet = new ConcurrentSkipListSet<>();
        position = request.getPosition();
        nack = false;

        count = 0;
        done = false;

        classicQuorum = ProcessDescriptor.getInstance().classicQuorum;
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

    public void addReply(SlowProposeReply msg, int sender) {
        replies[sender] = msg;
        count++;
        predSet.addAll(msg.getPred());
        if (!nack)
            nack = (msg.getStatus() == SlowProposeReply.Status.NACK);
        position = msg.position() > position ? msg.position() : position;
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
}
