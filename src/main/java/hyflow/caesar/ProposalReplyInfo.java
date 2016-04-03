package hyflow.caesar;

import hyflow.caesar.messages.ProposeReply;
import hyflow.common.ProcessDescriptor;
import hyflow.common.Request;
import hyflow.common.RequestId;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Created by balajiarun on 3/14/16.
 */
public class ProposalReplyInfo {

    private final Request request;
    private final ProposeReply[] replies;

    private final int fastQuorum;
    private final int classicQuorum;

    private final SortedSet<RequestId> predSet;
    private long position;
    private boolean nack;

    private int count;
    private boolean done;
    private Logger logger = LogManager.getLogger(ProposalReplyInfo.class);

    public ProposalReplyInfo(Request request, int numReplicas) {
        this.request = request;

        replies = new ProposeReply[numReplicas];

        predSet = new TreeSet<>();
        position = request.getPosition();
        nack = false;

        count = 0;
        done = false;

        classicQuorum = ProcessDescriptor.getInstance().classicQuorum;
        fastQuorum = ProcessDescriptor.getInstance().fastQuorum;

        logger.debug("Quorum: " + classicQuorum + ":" + fastQuorum);
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

    public void addReply(ProposeReply msg, int sender) {
        replies[sender] = msg;
        count++;
        predSet.addAll(msg.getPred());
        if (!nack)
            nack = (msg.getStatus() == ProposeReply.Status.NACK);
        position = msg.position() > position ? msg.position() : position;
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

}
