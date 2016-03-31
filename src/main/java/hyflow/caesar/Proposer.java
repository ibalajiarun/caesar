package hyflow.caesar;

import hyflow.caesar.messages.*;
import hyflow.caesar.network.Network;
import hyflow.common.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Queue;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;

class Proposer {

    private final TimestampGenerator tsGenerator;
    private final ConflictDetector conflictDetector;
    private final Network network;
    private final SingleThreadDispatcher dipatcher;
    private final Caesar caesar;

    private final ConcurrentMap<RequestId, ProposalReplyInfo> proposalReplies;
    private final ConcurrentMap<RequestId, RetryReplyInfo> retryReplies;

    private final ConcurrentHashMap<RequestId, Queue<Runnable>> proposeRunnables;
    private final ConcurrentHashMap<RequestId, Queue<Runnable>> deliverRunnables;

    private final Logger logger = LogManager.getLogger(Proposer.class);

    Proposer(TimestampGenerator tsGenerator, ConflictDetector conflictDetector,
             Network network, SingleThreadDispatcher dispatcher, Caesar caesar) {
        this.tsGenerator = tsGenerator;
        this.conflictDetector = conflictDetector;
        this.network = network;
        this.dipatcher = dispatcher;
        this.caesar = caesar;

        this.proposalReplies = new ConcurrentHashMap<>(100000);
        this.retryReplies = new ConcurrentHashMap<>(100000);

        this.proposeRunnables = new ConcurrentHashMap<>(100000);
        this.deliverRunnables = new ConcurrentHashMap<>(100000);
    }

    void propose(Request request) {
        request.setPosition(tsGenerator.newTimestamp());
        request.setView(0);
        sendPropose(0, request);
    }

    private void sendPropose(int view, Request request) {
        Propose proposeMsg = new Propose(view, request);

        proposalReplies.put(request.getId(),
                new ProposalReplyInfo(request, ProcessDescriptor.getInstance().numReplicas));

        logger.debug("Proposing");
        network.sendToAll(proposeMsg);
    }

    void onPropose(Propose msg, int sender) {
        logger.debug("onPropose");
        Request request = msg.getRequest();

        proposeRunnables.put(request.getId(), new ConcurrentLinkedQueue<>());

        synchronized (request) {
        }

        conflictDetector.putRequest(request);

        request.setStatus(RequestStatus.Pending);

        SortedSet<Request> waitReqs = new TreeSet<>();
        boolean shouldReject = conflictDetector.computeWaitSetOrReject(request, waitReqs);

        if (shouldReject) sendProposeReject(msg, sender);

        proposeResume(msg, sender, waitReqs);
    }

    private void sendProposeReject(Propose msg, int sender) {
        Request request = msg.getRequest();

        request.setStatus(RequestStatus.Rejected);

        long position = tsGenerator.newTimestamp();
        request.setPosition(position);

        Set<RequestId> predSet = conflictDetector.computeNewPredFor(request, position);
        request.setPred(predSet);

        ProposeReply replyMsg = new ProposeReply(msg.getView(), request, ProposeReply.Status.NACK);
        network.sendMessage(replyMsg, sender);
    }

    private void proposeResume(Propose msg, int sender, SortedSet<Request> waitReqs) {
        Request request = msg.getRequest();

        for (Request req : waitReqs) {
            if (req.getStatus() != RequestStatus.Accepted && req.getStatus() != RequestStatus.Stable) {
                Queue<Runnable> prQ = proposeRunnables.get(req.getId());

                assert prQ != null : "Propose Runnable was not created for " + req.getId();

                prQ.add(new OnProposeRunner(msg, sender, waitReqs));
                return;
            } else if (!req.getPred().contains(request.getId())) {
                sendProposeReject(msg, sender);
            }
        }

        Set<RequestId> predSet = conflictDetector.computeNewPredFor(request, request.getPosition());
        request.setPred(predSet);

        ProposeReply replyMsg = new ProposeReply(0, request, ProposeReply.Status.ACK);
        network.sendMessage(replyMsg, sender);
    }

    void onProposeReply(ProposeReply msg, int sender) {
        logger.debug("proposeReply");
        if (msg.position() > -1) {
            logger.debug("Updating TS to " + msg.position());
            tsGenerator.setTimestamp(msg.position());
        }

        RequestId rId = msg.getRequestId();
        ProposalReplyInfo info = proposalReplies.get(msg.getRequestId());

        assert info != null : "The info object is null for " + rId;

        synchronized (info) {
            info.addReply(msg, sender);

            if (info.isDone())
                return;

            if (!info.shouldRetry() && info.isFastQuorum()) {

                Stable stableMsg = new Stable(0, info.getRequest());
                logger.debug("stabling");
                network.sendToAll(stableMsg);

            } else if (info.isClassicQuorum()) {

                Request request = info.getRequest();
                request.setStatus(RequestStatus.Rejected);
                request.setPosition(tsGenerator.newTimestamp());
                Retry retryMsg = new Retry(0, info.getRequest());
                logger.debug("retrying");
                network.sendToAll(retryMsg);

            } else {
                return;
            }
            info.setDone();
        }
    }

    void onStable(Stable msg, int sender) {
        Request msgRequest = msg.getRequest();
        RequestId rId = msgRequest.getId();

        Request request = conflictDetector.getRequest(rId);

        request.setPosition(msgRequest.getPosition());
        request.setPred(msgRequest.getPred());
        request.setStatus(RequestStatus.Stable);

        logger.debug("resuming reqs for" + rId.toString() + "; size: " + proposeRunnables.get(rId).size());
        proposeRunnables.get(rId).forEach(dipatcher::submit);
        proposeRunnables.remove(rId);

        logger.debug("Message stabilized " + rId.toString());

        dipatcher.submit(() -> deliver(request));
    }

    private void deliver(Request request) {
        Set<RequestId> predSet = request.getPred();

        Queue<Runnable> deliverQ = deliverRunnables.putIfAbsent(request.getId(), new LinkedBlockingQueue<>());

        // TODO: There is a problem here if "id" is not present in conflictDetector.
        predSet.removeIf(id -> {
            Request predReq = conflictDetector.getRequest(id);
            return predReq != null &&
                    (predReq.getStatus() == RequestStatus.Delivered
                            || predReq.getStatus() == RequestStatus.Stable)
                    && predReq.getPosition() > request.getPosition();
        });

        predSet.forEach(predId -> {
            Request predReq = conflictDetector.getRequest(predId);
            if (predReq == null || predReq.getStatus() != RequestStatus.Delivered) {
                Queue<Runnable> queue = deliverRunnables.get(predId);
                if (queue == null) {
                    queue = deliverRunnables.putIfAbsent(predId, new LinkedBlockingQueue<>());
                }
                queue.add(new OnDeliverRunner(request));
            }
        });

        request.setStatus(RequestStatus.Delivered);

        assert deliverQ != null : "Why is delivery queue null here?";
        logger.debug("DeliveryQ Size: " + deliverQ);

        deliverQ.forEach(event -> {
            if (event != null) dipatcher.submit(event);
        });
    }

    public void onRetry(Retry msg, int sender) {
        logger.debug("retry");

        Request msgRequest = msg.getRequest();

        Request request = conflictDetector.getRequest(msgRequest.getId());

        // TODO: Fix this. This is not the protocol.
        assert request != null : "A retry without the propose. Fix me!";

        if (request != null) {
            request.setPosition(msgRequest.getPosition());
            request.setPred(msgRequest.getPred());
            request.setStatus(RequestStatus.Accepted);
        } else {
            logger.fatal("A retry without the propose. Fix me.");
        }

        SortedSet<RequestId> predSet = conflictDetector.computeNewPredFor(request, request.getPosition());

        request.setStatus(RequestStatus.Accepted);

        RetryReply replyMsg = new RetryReply(0, request.getId(), request.getPred());
        network.sendMessage(replyMsg, sender);
    }

    public void onRetryReply(RetryReply msg, int sender) {
        logger.debug("retryreply");

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
            Stable stableMsg = new Stable(0, info.getRequest());
            network.sendToAll(stableMsg);
        }
    }

    private class OnProposeRunner implements Runnable {

        private final Propose msg;
        private final int sender;
        private final SortedSet<Request> waitReqs;

        public OnProposeRunner(Propose msg, int sender, SortedSet<Request> waitReqs) {
            this.msg = msg;
            this.sender = sender;
            this.waitReqs = waitReqs;
        }

        @Override
        public void run() {
            proposeResume(msg, sender, waitReqs);
        }
    }

    private class OnDeliverRunner implements Runnable {

        private final Request request;

        public OnDeliverRunner(Request request) {
            this.request = request;
        }

        @Override
        public void run() {
            deliver(request);
        }
    }

}