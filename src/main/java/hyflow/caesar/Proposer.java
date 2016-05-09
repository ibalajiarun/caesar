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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
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
    private final ScheduledThreadDispatcher intDispatcher;
    private final Caesar caesar;

    private final Network proposeChannel;
    private final Network repliesChannel;
    private final Network stableChannel;
    private final Network otherChannel;

//    private final ConcurrentMap<RequestId, FastProposeReplyInfo> fpReplies;
//    private final ConcurrentMap<RequestId, SlowProposeReplyInfo> spReplies;
//
//    private final ConcurrentMap<RequestId, RetryReplyInfo> retryReplies;
//
//    private final ConcurrentMap<RequestId, RecoveryInfo> recoveryReplies;
//
//    private final ConcurrentMap<RequestId, Queue<Runnable>> proposeRunnables;
//    private final ConcurrentMap<RequestId, Queue<Runnable>> deliverRunnables;
//
//    private final ConcurrentMap<RequestId, RequestInfo> reqInfos;

    private final FastProposeReplyInfo[] fpReplies;
    private final SlowProposeReplyInfo[] spReplies;

    private final RetryReplyInfo[] retryReplies;

    private final RecoveryInfo[] recoveryReplies;

    private final Queue<Runnable>[] proposeRunnables;
    private final Queue<Runnable>[] deliverRunnables;

    private final RequestInfo[] reqInfos;

    private final int localId;
    private final int fpTimeout;
    private final int threadSleep;
    private final int numReplicas;
    private long fpTotal = 0, fpMax = 0, fpCount;
    private long fpgTotal = 0, fpgMax = 0, fpgCount;
    private long prTotal = 0, prMax = 0, prCount;
    private long riTotal = 0, riMax = 0, riCount;

    Proposer(TimestampGenerator tsGenerator, ConflictDetector conflictDetector,
             Network proposeChannel, Network repliesChannel, Network stableChannel, Network otherChannel,
             ScheduledThreadDispatcher dispatcher, Caesar caesar) {

        this.tsGenerator = tsGenerator;
        this.conflictDetector = conflictDetector;
        this.intDispatcher = dispatcher;
        this.caesar = caesar;

        this.proposeChannel = proposeChannel;
        this.repliesChannel = repliesChannel;
        this.stableChannel = stableChannel;
        this.otherChannel = otherChannel;

        ProcessDescriptor pd = ProcessDescriptor.getInstance();
        int mapSize = pd.proposerMapSize;
        localId = pd.localId;
        fpTimeout = pd.fpTimeout;
        threadSleep = pd.proposerSleep;
        this.numReplicas = pd.numReplicas;

//        this.fpReplies = new ConcurrentHashMap<>(mapSize);
//        this.spReplies = new ConcurrentHashMap<>(mapSize);
//
//        this.retryReplies = new ConcurrentHashMap<>(mapSize);
//        this.recoveryReplies = new ConcurrentHashMap<>(mapSize);
//
//        this.proposeRunnables = new ConcurrentHashMap<>(mapSize);
//        this.deliverRunnables = new ConcurrentHashMap<>(mapSize);
//
//        this.reqInfos = new ConcurrentHashMap<>(mapSize);

        this.fpReplies = new FastProposeReplyInfo[mapSize];
//        for (int i = 0; i < mapSize; i++) {
//            fpReplies[i] = new FastProposeReplyInfo(null, pd.numReplicas);
//        }
//
        this.spReplies = new SlowProposeReplyInfo[mapSize];
//        for (int i = 0; i < mapSize; i++) {
//            spReplies[i] = new SlowProposeReplyInfo(null, pd.numReplicas);
//        }
//
        this.retryReplies = new RetryReplyInfo[mapSize];
//        for (int i = 0; i < mapSize; i++) {
//            retryReplies[i] = new RetryReplyInfo(null, pd.numReplicas);
//        }
//
        this.recoveryReplies = new RecoveryInfo[mapSize];
//        for (int i = 0; i < mapSize; i++) {
//            recoveryReplies[i] = new RecoveryInfo(null, pd.numReplicas);
//        }

        this.proposeRunnables = new BlockingQueue[mapSize];
        for (int i = 0; i < mapSize; i++) {
            proposeRunnables[i] = new LinkedBlockingQueue<>();
        }

        this.deliverRunnables = new BlockingQueue[mapSize];
        for (int i = 0; i < mapSize; i++) {
            deliverRunnables[i] = new LinkedBlockingQueue<>();
        }

        this.reqInfos = new RequestInfo[mapSize];
        for (int i = 0; i < mapSize; i++) {
            reqInfos[i] = new RequestInfo();
        }
    }

    private int getIntId(RequestId rId) {
        return rId.getClientId() + (rId.getSeqNumber() * numReplicas);
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

//        long start = System.currentTimeMillis();

        int id = getIntId(request.getId());
        fpReplies[id] = new FastProposeReplyInfo(request, ProcessDescriptor.getInstance().numReplicas);

//        fpReplies.put(request.getIntId(),
//                new FastProposeReplyInfo(request, ProcessDescriptor.getInstance().numReplicas));

//        long duration = System.currentTimeMillis() - start;
//
//        fpTotal += duration;
//        fpMax = Math.max(duration, fpMax);
//        fpCount++;
//        if(fpCount % 5000 == 0) {
//            System.out.println(String.format("FP Replies Put: Avg: %f; Max %d ", fpTotal * 1.0 / fpCount, fpMax));
//            fpCount = 0;
//            fpTotal = 0;
//        }

        proposeChannel.sendToAll(proposeMsg);

        logger.exit();
    }

    void onFastPropose(FastPropose msg, int sender) {
        logger.entry(msg, sender);

        int view = msg.getView();

        Request msgRequest = msg.getRequest();
        RequestId rId = msgRequest.getId();

//        long start = System.currentTimeMillis();

        int id = getIntId(rId);
        synchronized (reqInfos[id]) {
            RequestInfo rInfo = reqInfos[id];

            if (rInfo.getId() == null) {
                rInfo.init(rId, view, RequestStatus.PreFastPending);
            }
//        }
//        RequestInfo reqInfo = reqInfos.computeIfAbsent(rId,
//                k -> new RequestInfo(rId, view, RequestStatus.PreFastPending));

//        long duration = System.currentTimeMillis() - start;
//
//        riTotal += duration;
//        riMax = Math.max(duration, riMax);
//        riCount++;
//        if(riCount % 5000 == 0) {
//            System.out.println(String.format("ReqInfos: Avg: %f; Max %d ", riTotal * 1.0 / riCount, riMax));
//            riCount = 0;
//            riTotal = 0;
//        }

//        synchronized (reqInfo) {

            if ((rInfo.getView() > view) || (rInfo.getView() == view
                    && rInfo.getStatusOrdinal() > RequestStatus.PreFastPending.ordinal())) {
                logger.debug(ON_PROPOSE, "Disregarding {} {}", msg, rInfo);
                logger.exit();
                return;
            }

//            start = System.currentTimeMillis();

//            proposeRunnables.computeIfAbsent(rId, k -> new ConcurrentLinkedQueue<>());

//            duration = System.currentTimeMillis() - start;
//
//            prTotal += duration;
//            prMax = Math.max(duration, prMax);
//            prCount++;
//            if(prCount % 5000 == 0) {
//                System.out.println(String.format("PropRuns: Avg: %f; Max %d ", prTotal * 1.0 / prCount, prMax));
//                prCount = 0;
//                prTotal = 0;
//            }

            Request request = conflictDetector.updateRequest(msgRequest);

            rInfo.setStatus(RequestStatus.FastPending);
            request.setStatus(RequestStatus.FastPending);

            assert request.getStatus() == RequestStatus.FastPending :
                    "Request not pending " + request + ";" + rInfo;

            SortedSet<Request> waitReqs = new TreeSet<>();
            boolean rejected = conflictDetector.computeWaitSetOrReject(request, waitReqs);

            if (rejected) {

                if (logger.isDebugEnabled()) {
                    logger.debug(ON_PROPOSE, "Rejected: {}; Wait Set: {}", request, waitReqs);
                }

                sendFastProposeReject(rInfo, view, sender, request);
            } else {
                fastProposeResume(rInfo, request, view, sender, msg.getWhiteList(), waitReqs, 0);
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

                    Queue<Runnable> prQ = proposeRunnables[getIntId(req.getId())];

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

            FastProposeReply replyMsg = new FastProposeReply(view, request.getId(),
                    FastProposeReply.Status.ACK, predSet, request.getPosition());
            repliesChannel.sendMessage(replyMsg, sender);
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
        repliesChannel.sendMessage(replyMsg, sender);

        logger.exit();
    }

    void onFastProposeReply(FastProposeReply msg, int sender) {
        logger.entry(msg, sender);

        RequestId rId = msg.getRequestId();
        int id = getIntId(rId);

//        long start = System.currentTimeMillis();

        FastProposeReplyInfo info = fpReplies[id];

//        long duration = System.currentTimeMillis() - start;
//
//        fpgTotal += duration;
//        fpgMax = Math.max(duration, fpgMax);
//        fpgCount++;
//        if(fpgCount % 5000 == 0) {
//            System.out.println(String.format("FP Replies Get: Avg: %f; Max %d ", fpgTotal * 1.0 / fpgCount, fpgMax));
//            fpgCount = 0;
//            fpgTotal = 0;
//        }

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

                RequestStats.getInstance().fpCount.incrementAndGet();

                Stable stableMsg = new Stable(msg.getView(), request);
                stableChannel.sendToAll(stableMsg);

            } else if (info.hasNack() && info.isClassicQuorum()) {

                Request request = info.updateAndGetRequest();

                tsGenerator.setTimestamp(info.getMaxPosition());

                request.setPosition(tsGenerator.newTimestamp());

                retryReplies[id] = new RetryReplyInfo(request, numReplicas);

                Retry retryMsg = new Retry(msg.getView(), request);
                proposeChannel.sendToAll(retryMsg);

            } else if (info.isClassicQuorum()) {

                Request request = info.updateAndGetRequest();

                info.setSlowProposeFuture(
                        intDispatcher.schedule(() -> sendSlowPropose(msg.getView(), request, info),
                                fpTimeout, TimeUnit.MILLISECONDS)
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

        spReplies[getIntId(request.getId())] =
                new SlowProposeReplyInfo(request, ProcessDescriptor.getInstance().numReplicas);

        proposeChannel.sendToAll(proposeMsg);

        logger.exit();
    }

    void onSlowPropose(SlowPropose msg, int sender) {
        logger.entry(msg, sender);

        int view = msg.getView();

        Request msgRequest = msg.getRequest();
        RequestId rId = msgRequest.getId();
        int id = getIntId(rId);

        RequestInfo reqInfo = reqInfos[id];
//                k -> new RequestInfo(rId, view, RequestStatus.PreSlowPending));

        synchronized (reqInfo) {

            if (reqInfo.getId() == null) {
                reqInfo.init(rId, view, RequestStatus.PreSlowPending);
            }

            if ((reqInfo.getView() > view) || (reqInfo.getView() == view
                    && reqInfo.getStatusOrdinal() > RequestStatus.PreSlowPending.ordinal())) {
                if (logger.isDebugEnabled())
                    logger.debug(ON_PROPOSE, "Disregarding {} {}", msg, reqInfo);

                logger.exit();
                return;
            }

//            proposeRunnables.computeIfAbsent(rId, k -> new ConcurrentLinkedQueue<>());

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

                    Queue<Runnable> prQ = proposeRunnables[getIntId(req.getId())];
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
            repliesChannel.sendMessage(replyMsg, sender);
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
        repliesChannel.sendMessage(replyMsg, sender);

        logger.exit();
    }

    void onSlowProposeReply(SlowProposeReply msg, int sender) {
        logger.entry(msg, sender);

        RequestId rId = msg.getRequestId();
        int id = getIntId(rId);
        SlowProposeReplyInfo info = spReplies[id];

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

                RequestStats.getInstance().spCount.getAndIncrement();

                Stable stableMsg = new Stable(msg.getView(), request);
                stableChannel.sendToAll(stableMsg);

            } else if (info.hasNack() && info.isClassicQuorum()) {

                tsGenerator.setTimestamp(info.getMaxPosition());

                request.setPosition(tsGenerator.newTimestamp());

                retryReplies[id] = new RetryReplyInfo(request, ProcessDescriptor.getInstance().numReplicas);

                Retry retryMsg = new Retry(msg.getView(), request);
                proposeChannel.sendToAll(retryMsg);

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
        int id = getIntId(rId);

        int view = msg.getView();

        RequestInfo reqInfo = reqInfos[id];
//                k -> new RequestInfo(rId, view, RequestStatus.PreAccepted));

        synchronized (reqInfo) {

            if (reqInfo.getId() == null) {
                reqInfo.init(rId, view, RequestStatus.PreAccepted);
            }

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

            Queue<Runnable> prQ = proposeRunnables[id];
//                    k -> new ConcurrentLinkedQueue<>());

            synchronized (prQ) {
                prQ.forEach(intDispatcher::submit);
//                proposeRunnables.remove(prQ);
            }

            RetryReply replyMsg = new RetryReply(view, rId, predSet);
            repliesChannel.sendMessage(replyMsg, sender);
        }
        logger.exit();
    }

    void onRetryReply(RetryReply msg, int sender) {
        logger.entry(msg, sender);

        RetryReplyInfo info = retryReplies[getIntId(msg.getRequestId())];

        synchronized (info) {
            info.addReply(msg, sender);

            if (!info.isClassicQuorum() || info.isDone()) {
                logger.exit();
                return;
            }

            RequestStats.getInstance().retryCount.getAndIncrement();
            info.setDone();
            if (logger.isTraceEnabled()) {
                logger.trace("sending stable for {}", info);
            }
            Stable stableMsg = new Stable(msg.getView(), info.updateAndGetRequest());
            stableChannel.sendToAll(stableMsg);
        }

        logger.exit();
    }

    void onStable(Stable msg, int sender) {
        logger.entry(msg, sender);

        Request msgRequest = msg.getRequest();
        RequestId rId = msgRequest.getId();
        int id = getIntId(rId);

        int view = msg.getView();

        RequestInfo reqInfo = reqInfos[id];

        synchronized (reqInfo) {

            if (reqInfo.getId() == null) {
                reqInfo.init(rId, view, RequestStatus.PreStable);
            }

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

            Queue<Runnable> prQ = proposeRunnables[id];
//                    k -> new ConcurrentLinkedQueue<>());

            synchronized (prQ) {
                prQ.forEach(intDispatcher::submit);
//                proposeRunnables.remove(prQ);
            }

            deliver(request);
//            caesar.deliver(request, null);
        }

        logger.exit();
    }

    private void deliver(Request request) {
        logger.entry(request);

        Set<RequestId> predSet = request.getPred();

        Queue<Runnable> postDelQ = deliverRunnables[getIntId(request.getId())];

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
//                .forEach(predReq -> predReq.getPred().remove(request.getIntId()));

        deliverResume(request, postDelQ, 0);
        logger.exit();
    }

    //TODO: CHECK THE CORRECTNESS HERE. BreakLoop P1 is not here. FIXED I THINK NOW
    private void deliverResume(Request request, Queue<Runnable> postDelQ, int startIdx) {
        logger.entry(request, postDelQ, startIdx);

        int index = 0;
        Set<RequestId> predSet = request.getPred();
//        assert predSet.size() == 0 : "Ops! Pred Set not empty for " + request;

        for (RequestId predId : predSet) {
            if (index >= startIdx) {
                Request predReq = conflictDetector.getRequest(predId);

                Queue<Runnable> queue = deliverRunnables[getIntId(predId)];

                synchronized (queue) {
                    if (predReq != null && predReq.getPosition() < request.getPosition()
                            && predReq.getStatus().ordinal() >= RequestStatus.Stable.ordinal()) {
                        if (predReq.getPred().remove(request.getId())) {
                            queue.forEach(intDispatcher::submit);
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

        if (logger.isDebugEnabled()) {
            logger.debug("Delivered {}", request);
        }
        caesar.deliver(request, postDelQ);

        logger.exit();
    }

    void onDelivery(Request request, Queue<Runnable> postDelQ) {
        logger.entry(request, postDelQ);
        request.setStatus(RequestStatus.Delivered);

        assert postDelQ != null : "Delivery queue null for " + request;

        synchronized (postDelQ) {
            postDelQ.forEach(intDispatcher::submit);
            postDelQ.clear();
        }
        logger.exit();
    }

    void startRecovery(int nodeId) {
        logger.entry(nodeId);
        if (logger.isFatalEnabled())
            logger.fatal(RECOVERY, "Trying to recovery {}", nodeId);

        int numReplicas = ProcessDescriptor.getInstance().numReplicas;

        Arrays.stream(reqInfos)
                .filter(rInfo -> rInfo.getId().getClientId() % numReplicas == nodeId
                        && rInfo.getStatusOrdinal() < RequestStatus.Stable.ordinal())
                .forEach(this::recover);

//        reqInfos.forEach((requestId, requestInfo) -> {
//            if (requestInfo.getId().getClientId() % numReplicas == nodeId
//                    && requestInfo.getStatusOrdinal() < RequestStatus.Stable.ordinal()) {
//
//                if (logger.isTraceEnabled())
//                    logger.trace(RECOVERY, "Recovering {}; ReqInfo", requestId, requestInfo);
//
//                recover(requestId, requestInfo);
//
//            }
//        });

        logger.exit();
    }

    private void recover(RequestInfo rInfo) {
        logger.entry(rInfo);
        RequestId rId = rInfo.getId();


        synchronized (rInfo) {
            int newView = rInfo.getView() + 1;

            recoveryReplies[getIntId(rId)] = new RecoveryInfo(conflictDetector.getRequest(rId),
                    ProcessDescriptor.getInstance().numReplicas);

            Recovery recoverMsg = new Recovery(newView, rId);
            otherChannel.sendToAll(recoverMsg);
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

        RequestInfo reqInfo = reqInfos[getIntId(rId)];
//                k -> new RequestInfo(rId, view, RequestStatus.Waiting));

        synchronized (reqInfo) {

            if (reqInfo.getId() == null) {
                reqInfo.init(rId, view, RequestStatus.Waiting);
            }

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
            otherChannel.sendMessage(replyMsg, sender);
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
        int id = getIntId(rId);

        RecoveryInfo info = recoveryReplies[id];
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
                    stableChannel.sendToAll(stableMsg);

                    logger.exit();
                    return;
                }

                reply = info.getReplyWithStatus(RequestStatus.Accepted, false);

                if (reply != null) {
                    Request newReq = new Request(rId, request.getObjectIds(), request.getPayload(),
                            reply.getPosition(), reply.getPred(), RequestStatus.Accepted, view);

                    retryReplies[id] = new RetryReplyInfo(newReq, ProcessDescriptor.getInstance().numReplicas);

                    Retry retryMsg = new Retry(view, newReq);
//                    logger.fatal(RECOVERY, "Sending {}", retryMsg);
                    proposeChannel.sendToAll(retryMsg);

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
