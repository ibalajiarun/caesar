package hyflow.caesar;

import hyflow.caesar.messages.ProposeReply;
import hyflow.common.ProcessDescriptor;
import hyflow.common.Request;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;

/**
 * Created by balajiarun on 3/14/16.
 */
public class ProposalReplyInfo {

    private final Request request;
    private final ProposeReply[] replies;

    private final int fastQuorum;
    private final int classicQuorum;

    private int count;
    private boolean done;
    private Logger logger = LogManager.getLogger(ProposalReplyInfo.class);

    public ProposalReplyInfo(Request request, int numReplicas) {
        this.request = request;

        replies = new ProposeReply[numReplicas];

        count = 0;
        done = false;

        classicQuorum = ProcessDescriptor.getInstance().classicQuorum;
        fastQuorum = ProcessDescriptor.getInstance().fastQuorum;
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

    public void addReply(ProposeReply msg, int sender) {
        replies[sender] = msg;
        count++;
        if (msg.getPred() != null) {
            request.getPred().addAll(msg.getPred());
        }
    }

    public boolean isFastQuorum() {
        return (count >= fastQuorum);
    }

    public boolean isClassicQuorum() {
        return (count >= classicQuorum);
    }

    public boolean shouldRetry() {
        return Arrays.stream(replies).anyMatch(reply ->
                reply != null && reply.getStatus() == ProposeReply.Status.NACK
        );
    }
}
