package hyflow.caesar;

import hyflow.caesar.messages.*;
import hyflow.caesar.network.Network;
import hyflow.common.ProcessDescriptor;
import hyflow.common.Request;
import hyflow.common.RequestId;
import hyflow.common.RequestStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class Proposer {

    private class OnProposeRunner implements Runnable {

        private final Propose msg;
        private final int sender;

        public OnProposeRunner(Propose msg, int sender) {
            this.msg = msg;
            this.sender = sender;
        }

        @Override
        public void run() {
            proposeResume(msg, sender);
        }
    }

    private final TimestampGenerator tsGenerator;
    private final ConflictDetector conflictDetector;
    private final Network network;

    private final ConcurrentMap<RequestId, ProposalReplyInfo> proposalReplies;
    private final ConcurrentMap<RequestId, RetryReplyInfo> retryReplies;

    private final ConcurrentHashMap<RequestId, Queue<Runnable>> proposeRunnables;

    public Proposer(TimestampGenerator tsGenerator, ConflictDetector conflictDetector, Network network) {
        this.tsGenerator = tsGenerator;
        this.conflictDetector = conflictDetector;
        this.network = network;
        this.proposalReplies = new ConcurrentHashMap<>();
        this.retryReplies = new ConcurrentHashMap<>();
        this.proposeRunnables = new ConcurrentHashMap<>();
    }

    public void propose(Request request) {
        request.setPosition(tsGenerator.getTimeStamp());
        Propose proposeMsg = new Propose(request);
        conflictDetector.putRequest(request);
        proposeRunnables.put(request.getRequestId(), new LinkedList<>());
        network.sendToAll(proposeMsg);
    }

    public void onPropose(Propose msg, int sender) {
        Request request = msg.getRequest();
        if(ProcessDescriptor.getInstance().localId != sender) {
            conflictDetector.putRequest(request);
            proposeRunnables.put(request.getRequestId(), new LinkedList<>());
        }
        request.setStatus(RequestStatus.Pending);

        proposeResume(msg, sender);
    }

    public void proposeResume(Propose msg, int sender) {
        Request request = msg.getRequest();

        Request r = conflictDetector.findWaitRequest(request);
        if(r != null) {
            proposeRunnables.get(r.getRequestId()).add(new OnProposeRunner(msg, sender));
            return;
        }

        ProposeReply replyMsg;
        if(conflictDetector.noConflictFor(request)) {
            conflictDetector.updatePred(request);
            replyMsg = new ProposeReply(request.getRequestId(), ProposeReply.Status.ACK, request.getPred(), -1);
        } else {
            request.setStatus(RequestStatus.Rejected);
            long position = conflictDetector.findHighestPosition(request);
            replyMsg = new ProposeReply(request.getRequestId(), ProposeReply.Status.NACK, null, position);
        }

        network.sendMessage(replyMsg, sender);
    }

    public void onProposeReply(ProposeReply msg, int sender) {
        if(msg.getMaxPosition() > -1) {
            tsGenerator.setTimestamp(msg.getMaxPosition());
        }

        RequestId rId = msg.getRequestId();
        if(!proposalReplies.containsKey(rId)) {
            Request req = conflictDetector.getRequest(rId);
            proposalReplies.putIfAbsent(rId, new ProposalReplyInfo(req));
        }
        ProposalReplyInfo info = proposalReplies.get(msg.getRequestId());

        synchronized (info) {
            info.addReply(msg, sender);

            if(!info.isFastQuorum() || info.isDone())
                return;

            info.setDone();
            if(info.shouldRetry()) {
                Request request = conflictDetector.getRequest(rId);
                request.setStatus(RequestStatus.Rejected);
                request.setPosition(tsGenerator.getTimeStamp());
                Retry retryMsg = new Retry(info.getRequest());
                network.sendToAll(retryMsg);
            } else {
                Stable stableMsg = new Stable(info.getRequest());
                network.sendToAll(stableMsg);
            }
        }
    }

    public void onStable(Stable msg, int sender) {
        Request request = msg.getRequest();
        if(ProcessDescriptor.getInstance().localId != sender)
            conflictDetector.putRequest(request);

        request.setStatus(RequestStatus.Stable);
    }

    public void onRetry(Retry msg, int sender) {
        Request request = msg.getRequest();
        if(ProcessDescriptor.getInstance().localId != sender)
            conflictDetector.putRequest(request);

        conflictDetector.updatePred(request);
        request.setStatus(RequestStatus.Accepted);

        RetryReply replyMsg = new RetryReply(request.getRequestId(), request.getPred());
        network.sendMessage(replyMsg, sender);
    }

    public void onRetryReply(RetryReply msg, int sender) {
        RequestId rId = msg.getRequestId();
        if(!retryReplies.containsKey(rId)) {
            Request req = conflictDetector.getRequest(rId);
            retryReplies.putIfAbsent(rId, new RetryReplyInfo(req));
        }
        RetryReplyInfo info = retryReplies.get(msg.getRequestId());

        synchronized (info) {
            info.addReply(msg, sender);

            if(!info.isQuorum() || info.isDone())
                return;

            info.setDone();
            Stable stableMsg = new Stable(info.getRequest());
            network.sendToAll(stableMsg);
        }
    }

    private final Logger logger = LogManager.getLogger(Proposer.class);
}