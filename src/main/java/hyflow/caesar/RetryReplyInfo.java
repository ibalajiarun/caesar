package hyflow.caesar;

import hyflow.caesar.messages.RetryReply;
import hyflow.common.ProcessDescriptor;
import hyflow.common.Request;
import hyflow.common.RequestId;
import hyflow.common.RequestStatus;

import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * Created by balajiarun on 3/15/16.
 */
public class RetryReplyInfo {

    private final Request request;
    private final RetryReply[] replies;

    private final TreeSet<RequestId> predSet;
    private final int classicQuorum;
    private long position;
    private int count;
    private boolean done;

    public RetryReplyInfo(Request request, int numReplicas) {
        this.request = request;
//        this.request.setStatus(RequestStatus.Accepted);

        replies = new RetryReply[numReplicas];

        predSet = new TreeSet<>();
        position = request.getPosition();

        count = 0;
        done = false;

        classicQuorum = ProcessDescriptor.getInstance().classicQuorum;
    }

    public boolean isDone() {
        return done;
    }

    public void setDone() {
        done = true;
    }

    public Request updateAndGetRequest() {
        request.setPred(predSet);
        return request;
    }

    public void addReply(RetryReply msg, int sender) {
        replies[sender] = msg;
        count++;
        predSet.addAll(msg.getPred());
    }

    public boolean isClassicQuorum() {
        return (count >= classicQuorum);
    }

    public long getPosition() {
        return position;
    }

}
