package hyflow.caesar;

import hyflow.caesar.messages.*;
import hyflow.caesar.network.Network;
import hyflow.caesar.statistics.RequestStats;
import hyflow.common.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * Created by balajiarun on 4/9/16.
 */
public class Proposer {

    private static final Logger logger = LogManager.getLogger(Proposer.class);
    private static final Marker ON_PROPOSE = MarkerManager.getMarker("ON_PROPOSE");
    private static final Marker PROPOSE_RESUME = MarkerManager.getMarker("PROPOSE_RESUME");
    private static final Marker ON_PROPOSE_REPLY = MarkerManager.getMarker("ON_PROPOSE_REPLY");
    private static final Marker STABLE = MarkerManager.getMarker("ON_STABLE");
    private static final Marker RECOVERY = MarkerManager.getMarker("ON_RECOVERY");

    private final TimestampGenerator tsGenerator;
    private final ConflictDetector conflictDetector;
    private final Network network;
    private final ThreadDispatcher dispatcher;
    private final Caesar caesar;

    private final ConcurrentMap<RequestId, FastProposeReplyInfo> fpReplies;
    private final ConcurrentMap<RequestId, SlowProposeReplyInfo> spReplies;

    private final ConcurrentMap<RequestId, RetryReplyInfo> retryReplies;

    private final ConcurrentMap<RequestId, RecoveryInfo> recoveryReplies;

    private final ConcurrentMap<RequestId, Queue<Runnable>> proposeRunnables;
    private final ConcurrentMap<RequestId, Queue<Runnable>> deliverRunnables;

    private final ConcurrentMap<RequestId, RequestInfo> reqInfos;

    private final int localId;

    Proposer(TimestampGenerator tsGenerator, ConflictDetector conflictDetector,
             Network network, ThreadDispatcher dispatcher, Caesar caesar) {
        this.tsGenerator = tsGenerator;
        this.conflictDetector = conflictDetector;
        this.network = network;
        this.dispatcher = dispatcher;
        this.caesar = caesar;

        int mapSize = ProcessDescriptor.getInstance().proposerMapSize;
        localId = ProcessDescriptor.getInstance().localId;

        this.fpReplies = new ConcurrentHashMap<>(mapSize);
        this.spReplies = new ConcurrentHashMap<>(mapSize);

        this.retryReplies = new ConcurrentHashMap<>(mapSize);
        this.recoveryReplies = new ConcurrentHashMap<>(mapSize);

        this.proposeRunnables = new ConcurrentHashMap<>(mapSize);
        this.deliverRunnables = new ConcurrentHashMap<>(mapSize);

        this.reqInfos = new ConcurrentHashMap<>(mapSize);

//        QueueMonitor.getInstance().registerMap("Propose Runnables", proposeRunnables);
//        QueueMonitor.getInstance().registerMap("Deliver Runnables", deliverRunnables);

    }

    void fastPropose(Request request) {
        logger.entry();

        request.setPosition(tsGenerator.newTimestamp());
        request.setView(0);
        sendFastPropose(0, request, null);

        logger.exit();
    }

    private void sendFastPropose(int view, Request request, Set<RequestId> whiteList) {
        logger.entry(view, request);

        FastPropose proposeMsg = new FastPropose(view, request, whiteList);

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

        RequestInfo reqInfo = reqInfos.computeIfAbsent(rId,
                k -> new RequestInfo(rId, view, RequestStatus.PreFastPending));

        synchronized (reqInfo) {

            if ((reqInfo.getView() > view) || (reqInfo.getView() == view
                    && reqInfo.getStatusOrdinal() > RequestStatus.PreFastPending.ordinal())) {
                logger.debug(ON_PROPOSE, "Disregarding {} {}", msg, reqInfo);
                logger.exit();
                return;
            }

            proposeRunnables.computeIfAbsent(rId, k -> new ConcurrentLinkedQueue<>());

            Request request = conflictDetector.updateRequest(msgRequest);

            reqInfo.setStatus(RequestStatus.FastPending);
            request.setStatus(RequestStatus.FastPending);

            assert request.getStatus() == RequestStatus.FastPending :
                    "Request not pending " + request + ";" + reqInfo;

            SortedSet<Request> waitReqs = new TreeSet<>();
            boolean rejected = conflictDetector.computeWaitSetOrReject(request, waitReqs);

            if (rejected) {

                if (logger.isDebugEnabled()) {
                    logger.debug(ON_PROPOSE, "Rejected: {}; Wait Set: {}", request, waitReqs);
                }

                sendFastProposeReject(reqInfo, view, sender, request);
            } else {
                fastProposeResume(reqInfo, request, view, sender, msg.getWhiteList(), waitReqs, 0);
            }
        }

        logger.exit();
    }

    private void fastProposeResume(RequestInfo reqInfo, Request request, int view, int sender,
                                   Set<RequestId> whiteList, SortedSet<Request> waitReqs, int startIdx) {
        logger.entry(request, view, sender, waitReqs, startIdx);

        synchronized (reqInfo) {

            if ((reqInfo.getView() > view) || (reqInfo.getView() == view
                    && reqInfo.getStatusOrdinal() > RequestStatus.FastPending.ordinal())) {
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

                            prQ.add(new OnFastProposeRunner(reqInfo, request, view, sender, whiteList,
                                    waitReqs, index));
                            if (logger.isDebugEnabled()) {
                                logger.debug("{} is going to wait for {}; WaitReqs {}", request,
                                        req, waitReqs);
                            }
                            logger.exit();
                            return;

                        } else if (!req.getPred().contains(request.getId())) {

                            if (logger.isTraceEnabled()) {
                                logger.trace("{} does not contain {}", req, request);
                            }
                            sendFastProposeReject(reqInfo, view, sender, request);
                            logger.exit();
                            return;

                        }
                    }

                }
                index++;
            }

            Set<RequestId> predSet = conflictDetector.computeNewPredFor(request,
                    request.getPosition(), whiteList);
            request.setPred(predSet);
            request.setHasWhitelist(whiteList != null);

            FastProposeReply replyMsg = new FastProposeReply(0, request.getId(),
                    FastProposeReply.Status.ACK, predSet, request.getPosition());
            network.sendMessage(replyMsg, sender);
        }

        logger.exit();
    }

    private void sendFastProposeReject(RequestInfo reqInfo, int view, int sender, Request request) {
        logger.entry(reqInfo, request, view, sender);

        request.setStatus(RequestStatus.Rejected);
        reqInfo.setStatus(RequestStatus.Rejected);

        long position = tsGenerator.newTimestamp();

        Set<RequestId> predSet = conflictDetector.computeNewPredFor(request, position, null);
        request.setHasWhitelist(false);


        FastProposeReply replyMsg = new FastProposeReply(view, request.getId(), FastProposeReply.Status.NACK, predSet, position);
        network.sendMessage(replyMsg, sender);

        logger.exit();
    }

    void onFastProposeReply(FastProposeReply msg, int sender) {
        logger.entry(msg, sender);

        RequestId rId = msg.getRequestId();
        FastProposeReplyInfo info = fpReplies.get(rId);

        // TODO: CHECK IF THIS AFFECTS CORRECTNESS
        if (info == null) {
            if (logger.isTraceEnabled())
                logger.trace("info is null for {}", rId);
            logger.exit();
            return;
        }

        synchronized (info) {
            if (info.isDone()) {
                if (logger.isTraceEnabled())
                    logger.trace("Already decided {}; Info {}", rId, info);
                logger.exit();
                return;
            }

            info.addReply(msg, sender);

            if (!info.hasNack() && info.isFastQuorum()) {

                if (logger.isDebugEnabled()) {
//                    Set<RequestId> pred = request.getPred();
//                    for (int i=0;i<rId.getSeqNumber();i++) {
//                        RequestId findId = new RequestId(rId.getClientId(), i);
//                        assert pred.contains(findId) : "Not found " + findId + " in " + rId + "Set " + pred;
//                    }
//                    assert pred.size() == rId.getSeqNumber() : request + " Pred Size " + pred.size();
                }

                Request request = info.updateAndGetRequest();

                Stable stableMsg = new Stable(msg.getView(), request);
                network.sendToAll(stableMsg);

            } else if (info.hasNack() && info.isClassicQuorum()) {

                Request request = info.updateAndGetRequest();

                tsGenerator.setTimestamp(info.getMaxPosition());

                request.setPosition(tsGenerator.newTimestamp());

                retryReplies.put(rId, new RetryReplyInfo(request, ProcessDescriptor.getInstance().numReplicas));

                Retry retryMsg = new Retry(msg.getView(), request);
                network.sendToAll(retryMsg);

            } else if (info.isClassicQuorum()) {

                Request request = info.updateAndGetRequest();

                info.setSlowProposeFuture(
                        dispatcher.schedule(() -> sendSlowPropose(msg.getView(), request, info),
                                100, TimeUnit.MILLISECONDS)
                );

                logger.exit();
                return;

            } else {

                logger.exit();
                return;

            }

            ScheduledFuture<?> future = info.getSlowProposeFuture();
            if (future != null) {
                future.cancel(false);
            }

            info.setDone();
        }

        logger.exit();
    }

    private void sendSlowPropose(int view, Request request, FastProposeReplyInfo fastInfo) {
        logger.entry(view, request, fastInfo);

        if (fastInfo != null) {
            synchronized (fastInfo) {
                if (fastInfo.isDone()) {
                    return;
                }
                fastInfo.setDone();
            }
        }

        if (logger.isTraceEnabled())
            logger.trace("Taking the slow phase for {}", request);

        SlowPropose proposeMsg = new SlowPropose(view, request);

        spReplies.put(request.getId(),
                new SlowProposeReplyInfo(request, ProcessDescriptor.getInstance().numReplicas));

        network.sendToAll(proposeMsg);

        logger.exit();
    }

    void onSlowPropose(SlowPropose msg, int sender) {
        logger.entry(msg, sender);
        RequestStats.getInstance().spCount.getAndIncrement();

        int view = msg.getView();

        Request msgRequest = msg.getRequest();
        RequestId rId = msgRequest.getId();

        RequestInfo reqInfo = reqInfos.computeIfAbsent(rId,
                k -> new RequestInfo(rId, view, RequestStatus.PreSlowPending));

        synchronized (reqInfo) {

            if ((reqInfo.getView() > view) || (reqInfo.getView() == view
                    && reqInfo.getStatusOrdinal() > RequestStatus.PreSlowPending.ordinal())) {
                if (logger.isDebugEnabled())
                    logger.debug(ON_PROPOSE, "Disregarding {} {}", msg, reqInfo);

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

    private void slowProposeResume(RequestInfo reqInfo, Request request, int view, int sender,
                                   SortedSet<Request> waitReqs, int startIdx) {
        logger.entry(request, sender, waitReqs, startIdx);

        synchronized (reqInfo) {

            if ((reqInfo.getView() > view) || (reqInfo.getView() == view
                    && reqInfo.getStatusOrdinal() > RequestStatus.SlowPending.ordinal())) {
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

                            if (logger.isDebugEnabled()) {
                                logger.debug("{} is going to wait for {}; WaitReqs {}", request,
                                        req, waitReqs);
                            }
                            prQ.add(new OnSlowProposeRunner(reqInfo, request, view, sender, waitReqs, index));
                            logger.exit();
                            return;

                        } else if (!req.getPred().contains(request.getId())) {

                            if (logger.isTraceEnabled())
                                logger.trace("{} does not contain {}", req, request);

                            sendSlowProposeReject(reqInfo, request, view, sender);
                            logger.exit();
                            return;

                        }
                    }

                }
                index++;
            }

            Set<RequestId> predSet = conflictDetector.computeNewPredFor(request, request.getPosition(), null);
            request.setPred(predSet);
            request.setHasWhitelist(false);

            SlowProposeReply replyMsg = new SlowProposeReply(0, request.getId(), SlowProposeReply.Status.ACK, predSet, request.getPosition());
            network.sendMessage(replyMsg, sender);
        }

        logger.exit();
    }

    private void sendSlowProposeReject(RequestInfo reqInfo, Request request, int view, int sender) {
        logger.entry(request, view, sender);

        reqInfo.setStatus(RequestStatus.Rejected);
        request.setStatus(RequestStatus.Rejected);

        long position = tsGenerator.newTimestamp();

        Set<RequestId> predSet = conflictDetector.computeNewPredFor(request, position, null);
        request.setHasWhitelist(false);

        SlowProposeReply replyMsg = new SlowProposeReply(view, request.getId(), SlowProposeReply.Status.NACK, predSet, position);
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

//                if (logger.isDebugEnabled()) {
//                    Set<RequestId> pred = request.getPred();
//                    for (int i = 0; i < rId.getSeqNumber(); i++) {
//                        RequestId findId = new RequestId(rId.getClientId(), i);
//                        assert pred.contains(findId) : "Not found " + findId + " in " + rId + "Set " + pred;
//                    }
//                    assert pred.size() == rId.getSeqNumber() : request + " Pred Size " + pred.size();
//                }

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
        RequestStats.getInstance().retryCount.getAndIncrement();

        Request msgRequest = msg.getRequest();
        RequestId rId = msgRequest.getId();

        int view = msg.getView();

        RequestInfo reqInfo = reqInfos.computeIfAbsent(rId,
                k -> new RequestInfo(rId, view, RequestStatus.PreAccepted));

        synchronized (reqInfo) {

            if ((reqInfo.getView() > view) || (reqInfo.getView() == view
                    && reqInfo.getStatusOrdinal() > RequestStatus.PreAccepted.ordinal())) {
                logger.exit();
                return;
            }

            Request request = conflictDetector.updateRequest(msgRequest);

            SortedSet<RequestId> predSet = conflictDetector.computeNewPredFor(request, request.getPosition(), null);
            predSet.addAll(request.getPred());

            request.setStatus(RequestStatus.Accepted);
            reqInfo.setStatus(RequestStatus.Accepted);

            request.setHasWhitelist(false);

            Queue<Runnable> prQ = proposeRunnables.computeIfAbsent(rId,
                    k -> new ConcurrentLinkedQueue<>());

            synchronized (prQ) {
                prQ.forEach(dispatcher::submit);
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
        int view = msg.getView();

        RequestInfo reqInfo = reqInfos.computeIfAbsent(rId,
                k -> new RequestInfo(rId, msg.getView(), RequestStatus.PreStable));

        synchronized (reqInfo) {

            if ((reqInfo.getView() > view) || (reqInfo.getView() == view
                    && reqInfo.getStatusOrdinal() > RequestStatus.PreStable.ordinal())) {
                logger.exit();
                return;
            }

            if (logger.isDebugEnabled())
                logger.debug(STABLE, "ReqInfo: {}", reqInfo);

            Request request = conflictDetector.updateRequest(msgRequest);
//            Set<RequestId> pred = msgRequest.getPred();
//            for (int i=0;i<rId.getSeqNumber();i++) {
//                RequestId findId = new RequestId(rId.getClientId(), i);
//                assert pred.contains(findId) : "Not found " + findId + " in " + rId + "Set " + pred;
//            }
            reqInfo.setStatus(RequestStatus.Stable);
            request.setStatus(RequestStatus.Stable);
            request.setHasWhitelist(false);

            Queue<Runnable> prQ = proposeRunnables.computeIfAbsent(rId,
                    k -> new ConcurrentLinkedQueue<>());

            synchronized (prQ) {
                prQ.forEach(dispatcher::submit);
                proposeRunnables.remove(prQ);
            }

            dispatcher.execute(() -> deliver(request));
//            caesar.deliver(request, null);
        }

        logger.exit();
    }

    private void deliver(Request request) {
        logger.entry(request);

        Set<RequestId> predSet = request.getPred();

        Queue<Runnable> postDelQ = deliverRunnables.computeIfAbsent(request.getId(),
                k -> new LinkedBlockingQueue<>());

        // TODO: There is a problem here if "id" is not present in conflictDetector.
        // Next step fixes this...
        predSet.removeIf(id -> {
            Request predReq = conflictDetector.getRequest(id);
            return predReq != null &&
                    (predReq.getStatus() == RequestStatus.Delivered
                            || predReq.getStatus() == RequestStatus.Stable)
                    && predReq.getPosition() > request.getPosition();
        });

//        predSet.parallelStream()
//                .map(conflictDetector::getRequest)
//                .filter(predReq -> predReq != null
//                        && predReq.getStatus().ordinal() >= RequestStatus.Stable.ordinal()
//                        && predReq.getPosition() < request.getPosition())
//                .forEach(predReq -> predReq.getPred().remove(request.getId()));

        deliverResume(request, postDelQ, 0);
        logger.exit();
    }

    //TODO: CHECK THE CORRECTNESS HERE. BreakLoop P1 is not here. FIXED I THINK NOW
    private void deliverResume(Request request, Queue<Runnable> postDelQ, int startIdx) {
        logger.entry(request, postDelQ, startIdx);

        int index = 0;
        Set<RequestId> predSet = request.getPred();

        for (RequestId predId : predSet) {
            if (index >= startIdx) {
                Request predReq = conflictDetector.getRequest(predId);

                Queue<Runnable> queue = deliverRunnables.computeIfAbsent(predId,
                        k -> new LinkedBlockingQueue<>());

                synchronized (queue) {
                    if (predReq != null && predReq.getPosition() < request.getPosition()
                            && predReq.getStatus().ordinal() >= RequestStatus.Stable.ordinal()) {
                        if (predReq.getPred().remove(request.getId())) {
                            queue.forEach(dispatcher::submit);
                            queue.clear();
                        }
                        continue;
                    }
                    if (predReq == null || predReq.getStatus() != RequestStatus.Delivered) {

                        queue.add(new OnDeliverRunner(request, postDelQ, index));
                        logger.debug("DeliverResume: {} going to sleep for {}", request, predReq);
                        logger.exit();
                        return;

                    }
                }
            }
            index++;
        }

        logger.debug("Delivered {}", request);
        caesar.deliver(request, postDelQ);

        logger.exit();
    }

    void onDelivery(Request request, Queue<Runnable> postDelQ) {
        logger.entry(request, postDelQ);
        request.setStatus(RequestStatus.Delivered);

        assert postDelQ != null : "Delivery queue null for " + request;

        synchronized (postDelQ) {
            postDelQ.forEach(dispatcher::submit);
            postDelQ.clear();
        }
        logger.exit();
    }

    void startRecovery(int nodeId) {
        logger.entry(nodeId);
        if (logger.isFatalEnabled())
            logger.fatal(RECOVERY, "Trying to recovery {}", nodeId);

        int numReplicas = ProcessDescriptor.getInstance().numReplicas;

        reqInfos.forEach((requestId, requestInfo) -> {
            if (requestInfo.getId().getClientId() % numReplicas == nodeId
                    && requestInfo.getStatusOrdinal() < RequestStatus.Stable.ordinal()) {

                if (logger.isTraceEnabled())
                    logger.trace(RECOVERY, "Recovering {}; ReqInfo", requestId, requestInfo);

                recover(requestId, requestInfo);

            }
        });

        logger.exit();
    }

    private void recover(RequestId rId, RequestInfo rInfo) {
        logger.entry(rId, rInfo);

        synchronized (rInfo) {
            int newView = rInfo.getView() + 1;

            recoveryReplies.put(rId, new RecoveryInfo(conflictDetector.getRequest(rId),
                    ProcessDescriptor.getInstance().numReplicas));

            Recovery recoverMsg = new Recovery(newView, rId);
            network.sendToAll(recoverMsg);
        }

        logger.exit();
    }

    void onRecovery(Recovery msg, int sender) {
        logger.entry(msg, sender);
        if (logger.isFatalEnabled())
            logger.fatal(RECOVERY, "Received onRecovery {} from {}", msg, sender);

        RequestStats.getInstance().recoverCount.getAndIncrement();

        RequestId rId = msg.getRequestId();
        int view = msg.getView();

        RequestInfo reqInfo = reqInfos.computeIfAbsent(rId,
                k -> new RequestInfo(rId, view, RequestStatus.Waiting));

        synchronized (reqInfo) {

            if (reqInfo.getView() > view) {
                if (logger.isFatalEnabled())
                    logger.fatal(RECOVERY, "Returning {}; {}", view, reqInfo);
                logger.exit();
                return;
            }

            Request request = conflictDetector.getRequest(rId);

            if (logger.isFatalEnabled())
                logger.fatal(RECOVERY, "OnRecovery for {}", request);

            RecoveryReply replyMsg = new RecoveryReply(view, rId, request);
            network.sendMessage(replyMsg, sender);
        }

        logger.exit();
    }

    void onRecoveryReply(RecoveryReply msg, int sender) {
        logger.entry(msg, sender);
        if (logger.isFatalEnabled())
            logger.fatal(RECOVERY, "Recovering {} from {}", msg, sender);

        RecoveryReply reply;

        int view = msg.getView();

        RequestId rId = msg.getRequestId();
        RecoveryInfo info = recoveryReplies.get(rId);
        Request request = conflictDetector.getRequest(rId);

        synchronized (info) {
            info.addReply(msg, sender);

            if (info.isClassicQuorum() && !info.isDone()) {
                info.setDone();

                if (logger.isFatalEnabled()) {
                    logger.fatal(RECOVERY, "Info for {}: {}", rId, info);
                }

                reply = info.getReplyWithStatus(RequestStatus.Stable, false);
                if (reply == null)
                    reply = info.getReplyWithStatus(RequestStatus.Delivered, false);

                if (reply != null) {
                    Request newReq = new Request(rId, request.getObjectIds(), request.getPayload(),
                            reply.getPosition(), reply.getPred(), RequestStatus.Stable, view);

                    Stable stableMsg = new Stable(view, newReq);
//                    logger.fatal(RECOVERY, "Sending {}", stableMsg);
                    network.sendToAll(stableMsg);

                    logger.exit();
                    return;
                }

                reply = info.getReplyWithStatus(RequestStatus.Accepted, false);

                if (reply != null) {
                    Request newReq = new Request(rId, request.getObjectIds(), request.getPayload(),
                            reply.getPosition(), reply.getPred(), RequestStatus.Accepted, view);

                    retryReplies.put(rId, new RetryReplyInfo(newReq, ProcessDescriptor.getInstance().numReplicas));

                    Retry retryMsg = new Retry(view, newReq);
//                    logger.fatal(RECOVERY, "Sending {}", retryMsg);
                    network.sendToAll(retryMsg);

                    logger.exit();
                    return;
                }

                reply = info.getReplyWithStatus(RequestStatus.Rejected, false);

                if (reply != null) {
                    Request newReq = new Request(rId, request.getObjectIds(), request.getPayload(),
                            tsGenerator.newTimestamp(), null, RequestStatus.PreFastPending, view);

                    Set<RequestId> whiteList = null;

//                    logger.fatal(RECOVERY, "Sending fast propose {} {} {}", view, newReq, whiteList);
                    sendFastPropose(view, newReq, whiteList);

                    logger.exit();
                    return;
                }

                reply = info.getReplyWithStatus(RequestStatus.SlowPending, false);

                if (reply != null) {
                    Request newReq = new Request(rId, request.getObjectIds(), request.getPayload(),
                            reply.getPosition(), reply.getPred(), RequestStatus.PreSlowPending, view);

//                    logger.fatal(RECOVERY, "Sending slow propose {} {}", view, newReq);
                    sendSlowPropose(view, newReq, null);

                    logger.exit();
                    return;
                }

                Set<RecoveryReply> recoverySet = info.getRecoverySet();

                Set<RequestId> predSet = recoverySet.stream()
                        .filter(r -> r.getStatus() == RequestStatus.FastPending)
                        .map(RecoveryReply::getPred)
                        .flatMap(Collection::stream)
                        .collect(Collectors.toSet());

                long position = recoverySet.stream()
                        .filter(r -> r.getStatus() == RequestStatus.FastPending)
                        .map(RecoveryReply::getPosition)
                        .findFirst()
                        .get();

                reply = info.getReplyWithStatus(RequestStatus.FastPending, true);

                Set<RequestId> whiteList;
                int classicQuorum = ProcessDescriptor.getInstance().classicQuorum;
                int majority = classicQuorum / 2 + 1;

                if (reply != null) {
                    whiteList = predSet;
                } else if (recoverySet.size() >= classicQuorum) {
                    whiteList = predSet.stream()
                            .filter(predId ->
                                    recoverySet.stream()
                                            .filter(recoveryReply ->
                                                    recoveryReply.getPred().contains(predId))
                                            .count() >= majority
                            )
                            .collect(Collectors.toSet());
                } else {
                    whiteList = null;
                }

                Request newReq = new Request(rId, request.getObjectIds(), request.getPayload(),
                        position, predSet, RequestStatus.PreFastPending, view);

//                logger.fatal(RECOVERY, "Sending Xfast proposeX {} {} {}", view, newReq, whiteList);

                sendFastPropose(view, newReq, whiteList);
            }
        }

        logger.exit();
    }

    private class OnFastProposeRunner implements Runnable {

        private final RequestInfo info;
        private final Request request;
        private final int sender;
        private final SortedSet<Request> waitReqs;
        private final int index;
        private final int view;
        private final Set<RequestId> whiteList;

        public OnFastProposeRunner(RequestInfo info, Request request, int view, int sender, Set<RequestId> whiteList, SortedSet<Request> waitReqs, int index) {
            this.info = info;
            this.request = request;
            this.view = view;
            this.sender = sender;
            this.waitReqs = waitReqs;
            this.index = index;
            this.whiteList = whiteList;
        }

        @Override
        public void run() {
            logger.trace("Calling fastProposeResume for {} and waitReq {}", request, waitReqs);
            fastProposeResume(info, request, view, sender, whiteList, waitReqs, index);
        }
    }

    private class OnSlowProposeRunner implements Runnable {

        private final RequestInfo info;
        private final Request request;
        private final int sender;
        private final SortedSet<Request> waitReqs;
        private final int index;
        private final int view;

        public OnSlowProposeRunner(RequestInfo info, Request request, int view, int sender, SortedSet<Request> waitReqs, int index) {
            this.info = info;
            this.request = request;
            this.view = view;
            this.sender = sender;
            this.waitReqs = waitReqs;
            this.index = index;
        }

        @Override
        public void run() {
            logger.trace("Calling slowProposeResume for {} and waitReq {}", request, waitReqs);
            slowProposeResume(info, request, view, sender, waitReqs, index);
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
            logger.debug("Delivery: {} {} {}", request, postDelQ, index);
            deliverResume(request, postDelQ, index);
        }
    }

}
