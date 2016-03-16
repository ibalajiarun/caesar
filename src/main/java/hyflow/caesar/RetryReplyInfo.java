package hyflow.caesar;

import hyflow.caesar.messages.RetryReply;
import hyflow.common.ProcessDescriptor;
import hyflow.common.Request;
import hyflow.common.RequestStatus;

/**
 * Created by balajiarun on 3/15/16.
 */
public class RetryReplyInfo {

    private final Request request;
    private final RetryReply[] replies;

    private int count;
    private boolean done;

    private final int quorum;

    public RetryReplyInfo(Request request) {
        this.request = request;
        this.request.setStatus(RequestStatus.Accepted);

        int numReplicas = ProcessDescriptor.getInstance().numReplicas;
        replies = new RetryReply[numReplicas];

        count = 0;
        done = false;

        int failures = (int) Math.floor(ProcessDescriptor.getInstance().numReplicas / 2.0) + 1;
        quorum = failures + 1;
    }

    public Request getRequest() {
        return request;
    }

    public boolean isDone() {
        return done;
    }

    public void setDone() {
        done = true;
    }

    public void addReply(RetryReply msg, int sender) {
        replies[sender] = msg;
        count++;
        request.getPred().addAll(msg.getPred());
    }

    public boolean isQuorum() {
        return (count >= quorum);
    }
}
