package hyflow.caesar;

import hyflow.caesar.messages.*;
import hyflow.caesar.network.Network;
import hyflow.common.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;

import java.util.Queue;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by balajiarun on 4/9/16.
 */
public class Proposer {

    private static final Logger logger = LogManager.getLogger(Proposer.class);
    private static final Marker ON_PROPOSE = MarkerManager.getMarker("ON_PROPOSE");
    private static final Marker PROPOSE_RESUME = MarkerManager.getMarker("PROPOSE_RESUME");
    private static final Marker ON_PROPOSE_REPLY = MarkerManager.getMarker("ON_PROPOSE_REPLY");
    private static final Marker STABLE = MarkerManager.getMarker("ON_STABLE");
    private final TimestampGenerator tsGenerator;
    private final ConflictDetector conflictDetector;
    private final Network network;
    private final ThreadDispatcher dipatcher;
    private final Caesar caesar;
    private final ConcurrentMap<RequestId, ProposalReplyInfo> proposalReplies;
    private final ConcurrentMap<RequestId, RetryReplyInfo> retryReplies;
    private final ConcurrentMap<RequestId, Queue<Runnable>> proposeRunnables;
    private final ConcurrentMap<RequestId, Queue<Runnable>> deliverRunnables;
    private final ConcurrentMap<RequestId, RequestInfo> ballots;
    private final int localId;

    public Proposer(TimestampGenerator tsGenerator, ConflictDetector conflictDetector,
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
    }

    void propose(Request request) {
        logger.entry();

        request.setPosition(tsGenerator.newTimestamp());
        request.setView(0);
        sendPropose(0, request);

        logger.exit();
    }

    private void sendPropose(int view, Request request) {
        logger.entry(view, request);

        Propose proposeMsg = new Propose(view, request);

        proposalReplies.put(request.getId(),
                new ProposalReplyInfo(request, ProcessDescriptor.getInstance().numReplicas));

        network.sendToAll(proposeMsg);

        logger.exit();
    }

    void onPropose(Propose msg, int sender) {
        logger.entry(msg, sender);

        int view = msg.getView();

        Request msgRequest = msg.getRequest();
        RequestId rId = msgRequest.getId();

        RequestInfo reqInfo = ballots.computeIfAbsent(rId, k -> new RequestInfo(rId, view, RequestStatus.PrePending));

        synchronized (reqInfo) {

            if (reqInfo.getView() > view
                    || reqInfo.getStatusOrdinal() > RequestStatus.PrePending.ordinal()) {
                logger.exit();
                return;
            }

            proposeRunnables.computeIfAbsent(rId, k -> new ConcurrentLinkedQueue<>());

            Request request = conflictDetector.updateRequest(msgRequest);

            reqInfo.setStatus(RequestStatus.Pending);
            request.setStatus(RequestStatus.Pending);

            assert request.getStatus() == RequestStatus.Pending : "Request not pending " + request + ";" + reqInfo
                    + ";" + System.identityHashCode(request);

            SortedSet<Request> waitReqs = new TreeSet<>();
            boolean rejected = conflictDetector.computeWaitSetOrReject(request, waitReqs);

            if (rejected) {
//                logger.fatal(ON_PROPOSE, "Rejected: {}\n Wait Set: {}", request, waitReqs);
                request.setStatus(RequestStatus.Rejected);
                reqInfo.setStatus(RequestStatus.Rejected);
                sendProposeReject(request, view, sender);
            } else {
                proposeResume(request, view, sender, waitReqs, 0);
            }
        }

        logger.exit();
    }

    private void proposeResume(Request request, int view, int sender, SortedSet<Request> waitReqs, int startIdx) {
        logger.entry(request, sender, waitReqs, startIdx);

        int index = 0;
        for (Request req : waitReqs) {
            if (index >= startIdx) {

                Queue<Runnable> prQ = proposeRunnables.get(req.getId());
                assert prQ != null : "prQ is null for " + request;

                synchronized (prQ) {

                    if (req.getStatus() != RequestStatus.Stable &&
                            req.getStatus() != RequestStatus.Accepted) {

                        prQ.add(new OnProposeRunner(request, view, sender, index, waitReqs));
                        logger.exit();
                        return;

                    } else if (!req.getPred().contains(request.getId())) {

                        sendProposeReject(request, view, sender);
                        logger.exit();
                        return;

                    }
                }

            }
            index++;
        }

        Set<RequestId> predSet = conflictDetector.computeNewPredFor(request, request.getPosition());
        request.setPred(predSet);

        ProposeReply replyMsg = new ProposeReply(0, request, ProposeReply.Status.ACK);
        network.sendMessage(replyMsg, sender);

        logger.exit();
    }

    private void sendProposeReject(Request request, int view, int sender) {
        logger.entry(request, view, sender);

        request.setStatus(RequestStatus.Rejected);

        long position = tsGenerator.newTimestamp();
        request.setPosition(position);

        Set<RequestId> predSet = conflictDetector.computeNewPredFor(request, position);
        request.setPred(predSet);

        ProposeReply replyMsg = new ProposeReply(view, request, ProposeReply.Status.NACK);
        network.sendMessage(replyMsg, sender);

        logger.exit();
    }

    void onProposeReply(ProposeReply msg, int sender) {
        logger.entry(msg, sender);

        RequestId rId = msg.getRequestId();
        ProposalReplyInfo info = proposalReplies.get(rId);

        // TODO: CHECK IF THIS AFFECTS CORRECTNESS
        if (info == null)
            return;

        synchronized (info) {
            if (info.isDone())
                return;

            info.addReply(msg, sender);

            Request request = info.updateAndGetRequest();

            if (!info.hasNack() && info.isFastQuorum()) {
//                Set<RequestId> pred = request.getPred();
//                for (int i=0;i<rId.getSeqNumber();i++) {
//                    RequestId findId = new RequestId(rId.getClientId(), i);
//                    assert pred.contains(findId) : "Not found " + findId + " in " + rId + "Set " + pred;
//                }
//                assert pred.size() == rId.getSeqNumber() : request + " Pred Size " + pred.size();
                Stable stableMsg = new Stable(msg.getView(), request);
                network.sendToAll(stableMsg);

            } else if (info.hasNack() && info.isClassicQuorum()) {

                tsGenerator.setTimestamp(info.getMaxPosition());

                request.setPosition(tsGenerator.newTimestamp());

                retryReplies.put(rId, new RetryReplyInfo(request, ProcessDescriptor.getInstance().numReplicas));

                Retry retryMsg = new Retry(msg.getView(), request);
                network.sendToAll(retryMsg);

            } else {

                logger.exit();
                return;

            }

            info.setDone();
        }

        logger.exit();
    }

    public void onRetry(Retry msg, int sender) {
        logger.entry(msg, sender);

        Request msgRequest = msg.getRequest();
        RequestId rId = msgRequest.getId();

        int view = msg.getView();

        RequestInfo reqInfo = ballots.computeIfAbsent(rId,
                k -> new RequestInfo(rId, view, RequestStatus.Accepted));

        synchronized (reqInfo) {

            if (reqInfo.getView() > view
                    || reqInfo.getStatusOrdinal() > RequestStatus.Accepted.ordinal()) {
                logger.exit();
                return;
            }

            Request request = conflictDetector.updateRequest(msgRequest);

            SortedSet<RequestId> predSet = conflictDetector.computeNewPredFor(request, request.getPosition());
            predSet.addAll(request.getPred());

            request.setStatus(RequestStatus.Accepted);
            reqInfo.setStatus(RequestStatus.Accepted);

            Queue<Runnable> prQ = proposeRunnables.computeIfAbsent(rId,
                    k -> new ConcurrentLinkedQueue<>());

            assert prQ != null : "This queue should not be null here";

            synchronized (prQ) {
                prQ.forEach(dipatcher::submit);
                prQ.clear();
            }

            RetryReply replyMsg = new RetryReply(view, rId, predSet);
            network.sendMessage(replyMsg, sender);
        }
    }

    public void onRetryReply(RetryReply msg, int sender) {
        logger.entry(msg, sender);

        RetryReplyInfo info = retryReplies.get(msg.getRequestId());

        synchronized (info) {
            info.addReply(msg, sender);

            if (!info.isClassicQuorum() || info.isDone())
                return;

            info.setDone();
            Stable stableMsg = new Stable(msg.getView(), info.updateAndGetRequest());
            network.sendToAll(stableMsg);
        }

        logger.exit();
    }

    void onStable(Stable msg, int sender) {
        logger.entry(msg, sender);

        Request msgRequest = msg.getRequest();
        RequestId rId = msgRequest.getId();

        RequestInfo reqInfo = ballots.computeIfAbsent(rId, k -> new RequestInfo(rId, msg.getView(), RequestStatus.Stable));

        synchronized (reqInfo) {

            if (reqInfo.getView() > msg.getView() ||
                    reqInfo.getStatusOrdinal() > RequestStatus.Stable.ordinal()) {
                logger.exit();
                return;
            }

            logger.debug(STABLE, "ReqInfo: {}", reqInfo);

            Request request = conflictDetector.updateRequest(msgRequest);
//            Set<RequestId> pred = msgRequest.getPred();
//            for (int i=0;i<rId.getSeqNumber();i++) {
//                RequestId findId = new RequestId(rId.getClientId(), i);
//                assert pred.contains(findId) : "Not found " + findId + " in " + rId + "Set " + pred;
//            }
            reqInfo.setStatus(RequestStatus.Stable);
            request.setStatus(RequestStatus.Stable);

            Queue<Runnable> prQ = proposeRunnables.computeIfAbsent(rId, k -> new ConcurrentLinkedQueue<>());

            synchronized (prQ) {
                prQ.forEach(dipatcher::submit);
                prQ.clear();
            }

//            dipatcher.execute(() -> deliver(request));
            caesar.deliver(request, null);
        }

        logger.exit();
    }

    private void deliver(Request request) {
        logger.entry(request);

        Set<RequestId> predSet = request.getPred();

        Queue<Runnable> postDelQ = deliverRunnables.computeIfAbsent(request.getId(), k -> new LinkedBlockingQueue<>());

        // TODO: There is a problem here if "id" is not present in conflictDetector.
        predSet.removeIf(id -> {
            Request predReq = conflictDetector.getRequest(id);
            return predReq != null &&
                    (predReq.getStatus() == RequestStatus.Delivered
                            || predReq.getStatus() == RequestStatus.Stable)
                    && predReq.getPosition() > request.getPosition();
        });

        deliverResume(request, postDelQ, 0);
    }

    //TODO: CHECK THE CORRECTNESS HERE. BreakLoop P1 is not here. FIXED I THINK NOW
    private void deliverResume(Request request, Queue<Runnable> postDelQ, int startIdx) {
        int index = 0;
        Set<RequestId> predSet = request.getPred();

        for (RequestId predId : predSet) {
            if (index >= startIdx) {
                Request predReq = conflictDetector.getRequest(predId);

                if (predReq != null && predReq.getPosition() < request.getPosition()) {
                    predReq.getPred().remove(request.getId());
                }

                Queue<Runnable> queue = deliverRunnables.computeIfAbsent(predId,
                        k -> new LinkedBlockingQueue<>());

                synchronized (queue) {
                    if (predReq == null || predReq.getStatus() != RequestStatus.Delivered) {

                        queue.add(new OnDeliverRunner(request, postDelQ, index));
                        logger.exit();
                        return;

                    }
                }
            }
            index++;
        }

        caesar.deliver(request, postDelQ);
    }

    public void onDelivery(Request request, Queue<Runnable> postDelQ) {
        logger.entry(request, postDelQ);
        request.setStatus(RequestStatus.Delivered);

//        assert postDelQ != null : "Delivery queue null for " + request;

//        synchronized (postDelQ) {
//            postDelQ.forEach(dipatcher::submit);
//            postDelQ.clear();
//        }
        logger.exit();
    }

    private class OnProposeRunner implements Runnable {

        private final Request request;
        private final int sender;
        private final SortedSet<Request> waitReqs;
        private final int index;
        private final int view;

        public OnProposeRunner(Request request, int view, int sender, int index, SortedSet<Request> waitReqs) {
            this.request = request;
            this.view = view;
            this.sender = sender;
            this.waitReqs = waitReqs;
            this.index = index;
        }

        @Override
        public void run() {
            proposeResume(request, view, sender, waitReqs, index);
        }
    }

    private class OnDeliverRunner implements Runnable {

        private final Request request;
        private final int index;
        private final Queue<Runnable> postDelQ;

        public OnDeliverRunner(Request request, Queue<Runnable> postDelQ, int index) {
            this.request = request;
            this.postDelQ = postDelQ;
            this.index = index;
        }

        @Override
        public void run() {
            deliverResume(request, postDelQ, index);
        }
    }

}
