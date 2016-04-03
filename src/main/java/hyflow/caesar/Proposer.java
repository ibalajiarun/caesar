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
    private final ThreadDispatcher dipatcher;
    private final Caesar caesar;

    private final ConcurrentMap<RequestId, ProposalReplyInfo> proposalReplies;
    private final ConcurrentMap<RequestId, RetryReplyInfo> retryReplies;

    private final ConcurrentHashMap<RequestId, Queue<Runnable>> proposeRunnables;
    private final ConcurrentHashMap<RequestId, Queue<Runnable>> deliverRunnables;

    private final ConcurrentMap<RequestId, RequestInfo> ballots;

    private final Logger logger = LogManager.getLogger(Proposer.class);
    private final int localId;

    Proposer(TimestampGenerator tsGenerator, ConflictDetector conflictDetector,
             Network network, ThreadDispatcher dispatcher, Caesar caesar) {
        this.tsGenerator = tsGenerator;
        this.conflictDetector = conflictDetector;
        this.network = network;
        this.dipatcher = dispatcher;
        this.caesar = caesar;

        int mapSize = ProcessDescriptor.getInstance().proposerMapSize;
        localId = ProcessDescriptor.getInstance().localId;

        this.proposalReplies = new ConcurrentHashMap<>(mapSize);
        this.retryReplies = new ConcurrentHashMap<>(mapSize);

        this.proposeRunnables = new ConcurrentHashMap<>(mapSize);
        this.deliverRunnables = new ConcurrentHashMap<>(mapSize);

        this.ballots = new ConcurrentHashMap<>(mapSize);

//        QueueMonitor monitor = QueueMonitor.getInstance();
//        monitor.registerMap("proposalReplies", proposalReplies);
//        monitor.registerMap("retryReplies", retryReplies);
//        monitor.registerMap("proposeRunnables", proposeRunnables);
//        monitor.registerMap("deliverRunnables", deliverRunnables);
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

        RequestInfo newReqInfo = new RequestInfo(msg.getView(), RequestStatus.PrePending);

        RequestInfo reqInfo = ballots.putIfAbsent(request.getId(), newReqInfo);
        if (reqInfo == null)
            reqInfo = newReqInfo;

        synchronized (reqInfo) {

            if (newReqInfo != reqInfo) {
                if (reqInfo.getView() > msg.getView() ||
                        reqInfo.getStatusOrdinal() > RequestStatus.PrePending.ordinal())
                    return;
            }

            proposeRunnables.put(request.getId(), new ConcurrentLinkedQueue<>());

            conflictDetector.updateRequest(request);

            request.setStatus(RequestStatus.Pending);
            reqInfo.setStatus(RequestStatus.Pending);

            SortedSet<Request> waitReqs = new TreeSet<>();
            boolean shouldReject = conflictDetector.computeWaitSetOrReject(request, waitReqs);

            if (shouldReject) {
                sendProposeReject(msg, sender);
                reqInfo.setStatus(RequestStatus.Rejected);
                return;
            }

            proposeResume(msg, sender, waitReqs);
        }
    }

    private void sendProposeReject(Propose msg, int sender) {
        logger.debug("Sending reject for" + msg.getRequest().getId());
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
                return;
            }
        }

        Set<RequestId> predSet = conflictDetector.computeNewPredFor(request, request.getPosition());
        request.setPred(predSet);

        ProposeReply replyMsg = new ProposeReply(0, request, ProposeReply.Status.ACK);
        network.sendMessage(replyMsg, sender);
    }

    void onProposeReply(ProposeReply msg, int sender) {
        logger.debug("proposeReply");

        RequestId rId = msg.getRequestId();
        ProposalReplyInfo info = proposalReplies.get(rId);

        assert info != null : "The info object is null for " + rId;

        synchronized (info) {
            info.addReply(msg, sender);

            if (info.isDone())
                return;

            Request request = info.updateAndGetRequest();

            if (!info.hasNack() && info.isFastQuorum()) {

                Stable stableMsg = new Stable(msg.getView(), request);
                logger.debug("stabling");
                network.sendToAll(stableMsg);

            } else if (info.hasNack() && info.isClassicQuorum()) {

                logger.debug("Retrying. Updating TS to " + info.getMaxPosition());
                tsGenerator.setTimestamp(info.getMaxPosition());

                request.setStatus(RequestStatus.Rejected);
                request.setPosition(tsGenerator.newTimestamp());

                retryReplies.put(rId, new RetryReplyInfo(request, ProcessDescriptor.getInstance().numReplicas));

                Retry retryMsg = new Retry(msg.getView(), request);
                logger.debug("retrying");
                network.sendToAll(retryMsg);

            } else {
                return;
            }
            info.setDone();
        }
    }

    void onStable(Stable msg, int sender) {
        logger.debug("onstable");

        Request msgRequest = msg.getRequest();
        RequestId rId = msgRequest.getId();

        RequestInfo newReqInfo = new RequestInfo(msg.getView(), RequestStatus.Stable);
        RequestInfo localInfo = ballots.putIfAbsent(rId, newReqInfo);
        if (localInfo == null)
            localInfo = newReqInfo;

        synchronized (localInfo) {

            if (newReqInfo != localInfo) {
                if (localInfo.getView() > msg.getView() ||
                        localInfo.getStatusOrdinal() > RequestStatus.Stable.ordinal()) {
                    logger.debug("Returning due to info mismatch: "
                            + localInfo + "; "
                            + msg);
                    return;
                }

            }

            assert msgRequest.getStatus() == RequestStatus.Stable : "Request is not stable";

            Request request = conflictDetector.updateRequest(msgRequest);
            localInfo.setStatus(RequestStatus.Stable);
            assert request.getStatus() == RequestStatus.Stable : "Req is not stable";

//            logger.debug("resuming reqs for" + rId.toString() + "; size: " + proposeRunnables.get(rId).size());

            Queue<Runnable> prQ = proposeRunnables.get(rId);
            assert prQ != null : "This queue should not be null here";
            synchronized (prQ) {
                prQ.forEach(dipatcher::submit);
            }

            logger.debug("Message stabilized " + rId.toString());

            dipatcher.submit(() -> deliver(request));

        }
    }

    private void deliver(Request request) {
        Set<RequestId> predSet = request.getPred();

        Queue<Runnable> newDeliverQ = new LinkedBlockingQueue<>();
        Queue<Runnable> deliverQ = deliverRunnables.putIfAbsent(request.getId(), newDeliverQ);
        if (deliverQ == null)
            deliverQ = newDeliverQ;

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
                Queue<Runnable> newQ = new LinkedBlockingQueue<>();
                Queue<Runnable> queue = deliverRunnables.putIfAbsent(predId, newQ);
                if (queue == null)
                    queue = newQ;
                queue.add(new OnDeliverRunner(request));
            }
        });

        caesar.deliver(request, deliverQ);
    }

    public void onRetry(Retry msg, int sender) {
        logger.debug("retry");

        Request msgRequest = msg.getRequest();

//        Request request = conflictDetector.updateAndGetRequest(msgRequest.getId());
//
//        // TODO: Fix this. This is not the protocol.
//        assert request != null : "A retry without the propose. Fix me!";
//
//        if (request != null) {
//            request.setPosition(msgRequest.getMaxPosition());
//            request.setPred(msgRequest.getPred());
//            request.setStatus(RequestStatus.Accepted);
//        } else {
//            logger.fatal("A retry without the propose. Fix me.");
//        }

        RequestInfo newReqInfo = new RequestInfo(msg.getView(), RequestStatus.Accepted);
        RequestInfo localInfo = ballots.putIfAbsent(msgRequest.getId(), newReqInfo);
        if (localInfo == null)
            localInfo = newReqInfo;

        synchronized (localInfo) {

            if (newReqInfo != localInfo) {
                if (localInfo.getView() > msg.getView() ||
                        localInfo.getStatusOrdinal() > RequestStatus.Accepted.ordinal())
                    return;
            }

            Request request = conflictDetector.updateRequest(msgRequest);

            SortedSet<RequestId> predSet = conflictDetector.computeNewPredFor(request, request.getPosition());

            request.setStatus(RequestStatus.Accepted);

            RetryReply replyMsg = new RetryReply(0, request.getId(), request.getPred());
            network.sendMessage(replyMsg, sender);
        }
    }

    public void onRetryReply(RetryReply msg, int sender) {
        logger.debug("retryreply");

//        RequestId rId = msg.getRequestId();
//        if(!retryReplies.containsKey(rId)) {
//            retryReplies.putIfAbsent(rId, new RetryReplyInfo(req));
//        }
//        Request req = conflictDetector.getRequest(rId);
        RetryReplyInfo info = retryReplies.get(msg.getRequestId());

        synchronized (info) {
            info.addReply(msg, sender);

            if (!info.isClassicQuorum() || info.isDone())
                return;

            info.setDone();
            Stable stableMsg = new Stable(msg.getView(), info.updateAndGetRequest());
            network.sendToAll(stableMsg);
        }
    }

    public void onDelivery(Request request, Queue<Runnable> deliverQ) {
        request.setStatus(RequestStatus.Delivered);

        assert deliverQ != null : "Why is delivery queue null here?";
        logger.debug("DeliveryQ Size: " + deliverQ);

        deliverQ.forEach(dipatcher::submit);
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