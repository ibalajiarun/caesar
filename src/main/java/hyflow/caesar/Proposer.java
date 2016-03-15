package hyflow.caesar;

import hyflow.caesar.messages.Propose;
import hyflow.caesar.messages.ProposeReply;
import hyflow.caesar.network.Network;
import hyflow.common.Request;
import hyflow.common.RequestId;
import hyflow.common.RequestStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class Proposer {

    private final TimestampGenerator tsGenerator;
    private final ConflictDetector conflictDetector;
    private final Network network;

    private final ConcurrentMap<RequestId, ProposalReplyInfo> proposalReplies;

    public Proposer(TimestampGenerator tsGenerator, ConflictDetector conflictDetector, Network network) {
        this.tsGenerator = tsGenerator;
        this.conflictDetector = conflictDetector;
        this.network = network;
        this.proposalReplies = new ConcurrentHashMap<>();
    }

    public void propose(Request request) {
        request.setPosition(tsGenerator.getTimeStamp());
        Propose proposeMsg = new Propose(request);
        network.sendToAll(proposeMsg);
    }

    public void onPropose(Request request, int sender) {
        conflictDetector.putRequest(request);
        request.setStatus(RequestStatus.Pending);
        conflictDetector.wait(request);
        ProposeReply replyMsg;
        if(conflictDetector.noConflictFor(request)) {
            replyMsg = new ProposeReply(request.getRequestId(), ProposeReply.Status.ACK, request.getPred(), -1);
        } else {
            request.setStatus(RequestStatus.Rejected);
            long position = conflictDetector.findHighestPosition(request);
            replyMsg = new ProposeReply(request.getRequestId(), ProposeReply.Status.NACK, null, position);
        }
        network.sendMessage(replyMsg, sender);
    }

    public void onProposeReply(ProposeReply msg, int sender) {
        Request request = conflictDetector.getRequest(msg.getRequestId());

        if(request.isProposeDone()) {
            return;
        }

        request.appendProposeReply(sender, msg.getStatus());
        request.getPred().addAll(msg.getPred());

        if(request.getProposeReplyCount() < 2 || request.isProposeDone())
            return;



    }

    private final Logger logger = LogManager.getLogger(Proposer.class);
}