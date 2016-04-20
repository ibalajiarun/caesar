package hyflow.caesar;

import hyflow.caesar.messages.*;
import hyflow.caesar.network.Network;
import hyflow.caesar.statistics.QueueMonitor;
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

    private final ConcurrentMap<RequestId, FastProposeReplyInfo> fpReplies;
    private final ConcurrentMap<RequestId, SlowProposeReplyInfo> spReplies;

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

        this.fpReplies = new ConcurrentHashMap<>(mapSize);
        this.spReplies = new ConcurrentHashMap<>(mapSize);

        this.retryReplies = new ConcurrentHashMap<>(mapSize);

        this.proposeRunnables = new ConcurrentHashMap<>(mapSize);
        this.deliverRunnables = new ConcurrentHashMap<>(mapSize);

        this.ballots = new ConcurrentHashMap<>(mapSize);

        QueueMonitor.getInstance().registerMap("Propose Runnables", proposeRunnables);
        QueueMonitor.getInstance().registerMap("Deliver Runnables", deliverRunnables);
    }

    void fastPropose(Request request) {
        logger.entry();

        request.setPosition(tsGenerator.newTimestamp());
        request.setView(0);
        sendFastPropose(0, request);

        logger.exit();
    }

    private void sendFastPropose(int view, Request request) {
        logger.entry(view, request);

        FastPropose proposeMsg = new FastPropose(view, request);

        fpReplies.put(request.getId(),
                new FastProposeReplyInfo(request, ProcessDescriptor.getInstance().numReplicas));

        network.sendToAll(proposeMsg);

        logger.exit();
    }

    void onFastPropose(FastPropose msg, int sender) {
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

            assert request.getStatus() == RequestStatus.Pending : "Request not pending " + request + ";" + reqInfo;

            SortedSet<Request> waitReqs = new TreeSet<>();
            boolean rejected = conflictDetector.computeWaitSetOrReject(request, waitReqs);

            if (rejected) {

                if (logger.isDebugEnabled()) {
                    logger.debug(ON_PROPOSE, "Rejected: {}; Wait Set: {}", request, waitReqs);
                }

                sendFastProposeReject(reqInfo, view, sender, request);
            } else {
                fastProposeResume(reqInfo, request, view, sender, waitReqs, 0);
            }
        }

        logger.exit();
    }

    private void fastProposeResume(RequestInfo reqInfo, Request request, int view, int sender, SortedSet<Request> waitReqs, int startIdx) {
        logger.entry(request, view, sender, waitReqs, startIdx);

        synchronized (reqInfo) {

            if (reqInfo.getView() > view
                    || reqInfo.getStatusOrdinal() > RequestStatus.Pending.ordinal()) {
                logger.exit();
                return;
            }

            int index = 0;
            for (Request req : waitReqs) {
                if (index >= startIdx) {

                    Queue<Runnable> prQ = proposeRunnables.get(req.getId());
                    assert prQ != null : "prQ is null for " + request;

                    synchronized (prQ) {

                        if (req.getStatus().ordinal() < RequestStatus.Accepted.ordinal()) {

                            prQ.add(new OnProposeRunner(reqInfo, request, view, sender, index, waitReqs));
                            if (logger.isDebugEnabled()) {
                                logger.debug("{} is going to wait for {}; WaitReqs {}", request, req, waitReqs);
                            }
                            logger.exit();
                            return;

                        } else if (!req.getPred().contains(request.getId())) {
                            logger.trace("{} does not contain {}", req, request);
                            sendFastProposeReject(reqInfo, view, sender, request);
                            logger.exit();
                            return;

                        }
                    }

                }
                index++;
            }

            Set<RequestId> predSet = conflictDetector.computeNewPredFor(request, request.getPosition());
            request.setPred(predSet);

            FastProposeReply replyMsg = new FastProposeReply(0, request, FastProposeReply.Status.ACK);
            network.sendMessage(replyMsg, sender);
        }

        logger.exit();
    }

    private void sendFastProposeReject(RequestInfo reqInfo, int view, int sender, Request request) {
        logger.entry(reqInfo, request, view, sender);

        request.setStatus(RequestStatus.Rejected);
        reqInfo.setStatus(RequestStatus.Rejected);

        long position = tsGenerator.newTimestamp();
        request.setPosition(position);

        Set<RequestId> predSet = conflictDetector.computeNewPredFor(request, position);
        request.setPred(predSet);

        FastProposeReply replyMsg = new FastProposeReply(view, request, FastProposeReply.Status.NACK);
        network.sendMessage(replyMsg, sender);

        logger.exit();
    }

    void onFastProposeReply(FastProposeReply msg, int sender) {
        logger.entry(msg, sender);

        RequestId rId = msg.getRequestId();
        FastProposeReplyInfo info = fpReplies.get(rId);

        // TODO: CHECK IF THIS AFFECTS CORRECTNESS
        if (info == null) {
            logger.trace("info is null for {}", rId);
            logger.exit();
            return;
        }

        synchronized (info) {
            if (info.isDone()) {
                logger.trace("Already decided {}; Info {}", rId, info);
                logger.exit();
                return;
            }

            info.addReply(msg, sender);

            Request request = info.updateAndGetRequest();

            if (!info.hasNack() && info.isFastQuorum()) {

                if (logger.isDebugEnabled()) {
//                    Set<RequestId> pred = request.getPred();
//                    for (int i=0;i<rId.getSeqNumber();i++) {
//                        RequestId findId = new RequestId(rId.getClientId(), i);
//                        assert pred.contains(findId) : "Not found " + findId + " in " + rId + "Set " + pred;
//                    }
//                    assert pred.size() == rId.getSeqNumber() : request + " Pred Size " + pred.size();
                }

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

    private void sendSlowPropose(int view, Request request) {
        logger.entry(view, request);

        SlowPropose proposeMsg = new SlowPropose(view, request);

        spReplies.put(request.getId(),
                new SlowProposeReplyInfo(request, ProcessDescriptor.getInstance().numReplicas));

        network.sendToAll(proposeMsg);

        logger.exit();
    }

    void onSlowPropose(SlowPropose msg, int sender) {
        logger.entry(msg, sender);

        int view = msg.getView();

        Request msgRequest = msg.getRequest();
        RequestId rId = msgRequest.getId();

        RequestInfo reqInfo = ballots.computeIfAbsent(rId, k -> new RequestInfo(rId, view, RequestStatus.SlowPrePending));

        synchronized (reqInfo) {

            if (reqInfo.getView() > view
                    || reqInfo.getStatusOrdinal() > RequestStatus.SlowPrePending.ordinal()) {
                logger.exit();
                return;
            }

            proposeRunnables.computeIfAbsent(rId, k -> new ConcurrentLinkedQueue<>());

            Request request = conflictDetector.updateRequest(msgRequest);

            reqInfo.setStatus(RequestStatus.SlowPending);
            request.setStatus(RequestStatus.SlowPending);

            assert request.getStatus() == RequestStatus.SlowPending : "Request not pending " + request + ";" + reqInfo;

            SortedSet<Request> waitReqs = new TreeSet<>();
            boolean rejected = conflictDetector.computeWaitSetOrReject(request, waitReqs);

            if (rejected) {

                if (logger.isDebugEnabled()) {
                    logger.debug(ON_PROPOSE, "Rejected: {}; Wait Set: {}", request, waitReqs);
                }

                sendSlowProposeReject(reqInfo, request, view, sender);
            } else {
                slowProposeResume(reqInfo, request, view, sender, waitReqs, 0);
            }
        }

        logger.exit();
    }

    private void slowProposeResume(RequestInfo reqInfo, Request request, int view, int sender, SortedSet<Request> waitReqs, int startIdx) {
        logger.entry(request, sender, waitReqs, startIdx);

        synchronized (reqInfo) {

            if (reqInfo.getView() > view
                    || reqInfo.getStatusOrdinal() > RequestStatus.SlowPending.ordinal()) {
                logger.exit();
                return;
            }

            int index = 0;
            for (Request req : waitReqs) {
                if (index >= startIdx) {

                    Queue<Runnable> prQ = proposeRunnables.get(req.getId());
                    assert prQ != null : "prQ is null for " + request;

                    synchronized (prQ) {

                        if (req.getStatus() != RequestStatus.Stable &&
                                req.getStatus() != RequestStatus.Accepted) {

                            prQ.add(new OnProposeRunner(reqInfo, request, view, sender, index, waitReqs));
                            logger.exit();
                            return;

                        } else if (!req.getPred().contains(request.getId())) {

                            sendSlowProposeReject(reqInfo, request, view, sender);
                            logger.exit();
                            return;

                        }
                    }

                }
                index++;
            }

            Set<RequestId> predSet = conflictDetector.computeNewPredFor(request, request.getPosition());
            request.setPred(predSet);

            FastProposeReply replyMsg = new FastProposeReply(0, request, FastProposeReply.Status.ACK);
            network.sendMessage(replyMsg, sender);
        }

        logger.exit();
    }

    private void sendSlowProposeReject(RequestInfo reqInfo, Request request, int view, int sender) {
        logger.entry(request, view, sender);

        reqInfo.setStatus(RequestStatus.Rejected);
        request.setStatus(RequestStatus.Rejected);

        long position = tsGenerator.newTimestamp();
        request.setPosition(position);

        Set<RequestId> predSet = conflictDetector.computeNewPredFor(request, position);
        request.setPred(predSet);

        SlowProposeReply replyMsg = new SlowProposeReply(view, request, SlowProposeReply.Status.NACK);
        network.sendMessage(replyMsg, sender);

        logger.exit();
    }

    void onSlowProposeReply(SlowProposeReply msg, int sender) {
        logger.entry(msg, sender);

        RequestId rId = msg.getRequestId();
        SlowProposeReplyInfo info = spReplies.get(rId);

        // TODO: CHECK IF THIS AFFECTS CORRECTNESS
        if (info == null)
            return;

        synchronized (info) {
            if (info.isDone())
                return;

            info.addReply(msg, sender);

            Request request = info.updateAndGetRequest();

            if (!info.hasNack() && info.isClassicQuorum()) {

                if (logger.isDebugEnabled()) {
                    Set<RequestId> pred = request.getPred();
                    for (int i = 0; i < rId.getSeqNumber(); i++) {
                        RequestId findId = new RequestId(rId.getClientId(), i);
                        assert pred.contains(findId) : "Not found " + findId + " in " + rId + "Set " + pred;
                    }
                    assert pred.size() == rId.getSeqNumber() : request + " Pred Size " + pred.size();
                }

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

    void onRetry(Retry msg, int sender) {
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
                proposeRunnables.remove(prQ);
            }

            RetryReply replyMsg = new RetryReply(view, rId, predSet);
            network.sendMessage(replyMsg, sender);
        }
        logger.exit();
    }

    void onRetryReply(RetryReply msg, int sender) {
        logger.entry(msg, sender);

        RetryReplyInfo info = retryReplies.get(msg.getRequestId());

        synchronized (info) {
            info.addReply(msg, sender);

            if (!info.isClassicQuorum() || info.isDone()) {
                logger.exit();
                return;
            }

            info.setDone();
            if (logger.isTraceEnabled()) {
                logger.trace("sending stable for {}", info);
            }
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
                proposeRunnables.remove(prQ);
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
//        request.setStatus(RequestStatus.Delivered);

//        assert postDelQ != null : "Delivery queue null for " + request;

//        synchronized (postDelQ) {
//            postDelQ.forEach(dipatcher::submit);
//            postDelQ.clear();
//        }
        logger.exit();
    }

    private class OnProposeRunner implements Runnable {

        private final RequestInfo info;
        private final Request request;
        private final int sender;
        private final SortedSet<Request> waitReqs;
        private final int index;
        private final int view;

        public OnProposeRunner(RequestInfo info, Request request, int view, int sender, int index, SortedSet<Request> waitReqs) {
            this.info = info;
            this.request = request;
            this.view = view;
            this.sender = sender;
            this.waitReqs = waitReqs;
            this.index = index;
        }

        @Override
        public void run() {
            logger.trace("Calling fastProposeResume for {} and waitReq {}", request, waitReqs);
            fastProposeResume(info, request, view, sender, waitReqs, index);
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
