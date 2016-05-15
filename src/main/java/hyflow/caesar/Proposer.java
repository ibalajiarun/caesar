package hyflow.caesar;

import hyflow.caesar.messages.*;
import hyflow.caesar.network.Network;
import hyflow.caesar.statistics.RequestStats;
import hyflow.common.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager;
import org.apache.logging.log4j.core.appender.SyslogAppender;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * Created by balajiarun on 4/9/16.
 */
public class Proposer {

    private static final Logger logger = LogManager.getLogger(Proposer.class);
//    private static final Marker ON_PROPOSE = MarkerManager.getMarker("ON_PROPOSE");
//    private static final Marker PROPOSE_RESUME = MarkerManager.getMarker("PROPOSE_RESUME");
//    private static final Marker ON_PROPOSE_REPLY = MarkerManager.getMarker("ON_PROPOSE_REPLY");
//    private static final Marker STABLE = MarkerManager.getMarker("ON_STABLE");
//    private static final Marker RECOVERY = MarkerManager.getMarker("ON_RECOVERY");

    private final TimestampGenerator tsGenerator;
    private final ConflictDetector conflictDetector;
    private final ScheduledThreadDispatcher intDispatcher;
    private final Caesar caesar;

    private final Network proposeChannel;
    private final Network repliesChannel;
    private final Network stableChannel;
    private final Network otherChannel;

    private final FastProposeReplyInfo[] fpReplies;
    private final SlowProposeReplyInfo[] spReplies;

    private final RetryReplyInfo[] retryReplies;

    private final RecoveryInfo[] recoveryReplies;

    private final Queue<Runnable>[] proposeRunnables;
    private final ConcurrentMap<RequestId, Runnable>[] deliverRunnables;

    private final RequestInfo[] reqInfos;

    private final Request[] proposedReqs;

    private final int localId;
    private final int fpTimeout;
    private final int threadSleep;
    private final int numReplicas;

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

        this.fpReplies = new FastProposeReplyInfo[mapSize];

        this.spReplies = new SlowProposeReplyInfo[mapSize];

        this.retryReplies = new RetryReplyInfo[mapSize];

        this.recoveryReplies = new RecoveryInfo[mapSize];

        this.proposeRunnables = new BlockingQueue[mapSize];
        for (int i = 0; i < mapSize; i++) {
            proposeRunnables[i] = new LinkedBlockingQueue<>();
        }

        this.deliverRunnables = new ConcurrentMap[mapSize];
        for (int i = 0; i < mapSize; i++) {
            deliverRunnables[i] = new ConcurrentHashMap<>(mapSize / 4);
        }

        this.reqInfos = new RequestInfo[mapSize];
        for (int i = 0; i < mapSize; i++) {
            reqInfos[i] = new RequestInfo();
        }

        this.proposedReqs = new Request[mapSize];
    }

    private int getIntId(RequestId rId) {
        return rId.getClientId() + (rId.getSeqNumber() * numReplicas);
    }

    void fastPropose(Request request) {
//        logger.entry();

        request.setPosition(tsGenerator.newTimestamp());
        request.setView(0);
        sendFastPropose(0, request, null);

//        try {
//            Thread.sleep(threadSleep);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }

//        logger.exit();
    }

    private void sendFastPropose(int view, Request request, Set<RequestId> whiteList) {
//        logger.entry(view, request);

        FastPropose proposeMsg = new FastPropose(view, request, whiteList);

        int id = getIntId(request.getId());
        fpReplies[id] = new FastProposeReplyInfo(request, ProcessDescriptor.getInstance().numReplicas);
        this.proposedReqs[id] = request;

        proposeChannel.sendToAll(proposeMsg);
//        request.sentTime = System.currentTimeMillis();
//        if(logger.isDebugEnabled()) {
//            logger.debug("Propose: {} ({})", request.getId(), request.getPosition());
//        }

//        logger.exit();
    }

    void onFastPropose(FastPropose msg, int sender) {
//        logger.entry(msg, sender);

        long start = System.currentTimeMillis();

        int view = msg.getView();

        Request msgRequest = msg.getRequest();
        RequestId rId = msgRequest.getId();

        int id = getIntId(rId);
        synchronized (reqInfos[id]) {
            RequestInfo rInfo = reqInfos[id];

            if (rInfo.getId() == null) {
                rInfo.init(rId, view, RequestStatus.PreFastPending);
            }


            if ((rInfo.getView() > view) || (rInfo.getView() == view
                    && rInfo.getStatusOrdinal() > RequestStatus.FastPending.ordinal())) {
//                logger.debug(ON_PROPOSE, "Disregarding {} {}", msg, rInfo);
//                logger.exit();
                return;
            }

            Request request = conflictDetector.updateRequest(msgRequest);

            rInfo.setStatus(RequestStatus.FastPending);
            request.setStatus(RequestStatus.FastPending);

            assert request.getStatus() == RequestStatus.FastPending :
                    "Request not pending " + request + ";" + rInfo;

            SortedSet<Request> waitReqs = new TreeSet<>();
            boolean rejected = conflictDetector.computeWaitSetOrReject(request, waitReqs);

//            if(waitReqs.size() > 0) {
//////                FastProposeReply replyMsg = new FastProposeReply(view, request.getId(),
//////                        FastProposeReply.Status.REPROPOSE, new TreeSet<>(), request.getPosition());
////////                System.out.println("Asking for repropose (" + request.getPosition() + ") " + waitReqs.size());
//////                repliesChannel.sendMessage(replyMsg, sender);
//                sendFastProposeReject(rInfo, view, sender, request);
//            }
//            else
            if (rejected) {
//                if (logger.isDebugEnabled()) {
//                    logger.debug(ON_PROPOSE, "Rejected: {}; Wait Set: {}", request, waitReqs);
//                }
                sendFastProposeReject(rInfo, view, sender, request);
            } else {
                fastProposeResume(rInfo, request, view, sender, msg.getWhiteList(), waitReqs, 0);
            }

//            request.onProposeDuration = (int) (System.currentTimeMillis() - start);
        }

//        logger.exit();
    }

    private void fastProposeResume(RequestInfo reqInfo, Request request, int view, int sender,
                                   Set<RequestId> whiteList, SortedSet<Request> waitReqs, int startIdx) {
//        logger.entry(request, view, sender, waitReqs, startIdx);

//        if(waitReqs.size() > 20) {
//            sendFastProposeReject(reqInfo, view, sender, request);
//            return;
//        }

        long curr = System.currentTimeMillis();
//        if(request.startWait > 0) {
//            request.waitDuration += curr - request.startWait;
//            request.startWait = 0;
//        }

        synchronized (reqInfo) {

            if ((reqInfo.getView() > view) || (reqInfo.getView() == view
                    && reqInfo.getStatusOrdinal() > RequestStatus.FastPending.ordinal())) {
//                logger.exit();
                return;
            }

            int index = 0;
            for (Request req : waitReqs) {
                if (index >= startIdx) {

                    Queue<Runnable> prQ = proposeRunnables[getIntId(req.getId())];

                    assert prQ != null : "prQ is null for " + request;

                    synchronized (prQ) {

                        if (request.getPosition() < req.getPosition()
                                && !req.getPred().contains(request.getId())) {

                            if (req.getStatus().ordinal() < RequestStatus.Accepted.ordinal()) {

                                prQ.add(new OnFastProposeRunner(reqInfo, request, view, sender, whiteList,
                                        waitReqs, index));
//                                request.startWait = System.currentTimeMillis();
//                            if (logger.isDebugEnabled()) {
//                                logger.debug("{} is going to wait for {}; WaitReqs {}", request,
//                                        req, waitReqs);
//                            }
//                            logger.exit();
                                return;

                            } else {

//                            if (logger.isTraceEnabled()) {
//                                logger.trace("{} does not contain {}", req, request);
//                            }
                                sendFastProposeReject(reqInfo, view, sender, request);
//                            logger.exit();
                                return;

                            }
                        }
                    }

                }
                index++;
            }

            Set<RequestId> predSet = conflictDetector.computeNewPredFor(request,
                    request.getPosition(), whiteList);
            request.setPred(predSet);
            request.setHasWhitelist(whiteList != null);

            Queue<Runnable> prQ = proposeRunnables[getIntId(request.getId())];
            synchronized (prQ) {
                prQ.forEach(intDispatcher::submit);
                prQ.clear();
            }

            FastProposeReply replyMsg = new FastProposeReply(view, request.getId(),
                    FastProposeReply.Status.ACK, predSet, request.getPosition());
            repliesChannel.sendMessage(replyMsg, sender);
        }

//        logger.exit();
    }

    private void sendFastProposeReject(RequestInfo reqInfo, int view, int sender, Request request) {
//        logger.entry(reqInfo, request, view, sender);

        request.setStatus(RequestStatus.Rejected);
        reqInfo.setStatus(RequestStatus.Rejected);

        long position = tsGenerator.newTimestamp();

        Set<RequestId> predSet = conflictDetector.computeNewPredFor(request, position, null);
        request.setHasWhitelist(false);


        FastProposeReply replyMsg = new FastProposeReply(view, request.getId(),
                FastProposeReply.Status.NACK, predSet, position);
        repliesChannel.sendMessage(replyMsg, sender);

//        logger.exit();
    }

    void onFastProposeReply(FastProposeReply msg, int sender) {
//        logger.entry(msg, sender);

        RequestId rId = msg.getRequestId();
        int id = getIntId(rId);

        if (msg.getStatus() == FastProposeReply.Status.REPROPOSE) {
            fastPropose(proposedReqs[id]);
            return;
        }

        FastProposeReplyInfo info = fpReplies[id];

        // TODO: CHECK IF THIS AFFECTS CORRECTNESS
        if (info == null) {
//            if (logger.isTraceEnabled())
//                logger.trace("info is null for {}", rId);
//            logger.exit();
            return;
        }

        synchronized (info) {
            if (info.isDone()) {
//                if (logger.isTraceEnabled())
//                    logger.trace("Already decided {}; Info {}", rId, info);
//                logger.exit();
                return;
            }

            info.addReply(msg, sender);

            if (!info.hasNack() && info.isFastQuorum()) {

//                if (logger.isDebugEnabled()) {
//                    Set<RequestId> pred = request.getPred();
//                    for (int i=0;i<rId.getSeqNumber();i++) {
//                        RequestId findId = new RequestId(rId.getClientId(), i);
//                        assert pred.contains(findId) : "Not found " + findId + " in " + rId + "Set " + pred;
//                    }
//                    assert pred.size() == rId.getSeqNumber() : request + " Pred Size " + pred.size();
//                }
//                logger.debug("Duration1: {}", System.currentTimeMillis() - proposedReqs[id].sentTime);

                Request request = info.updateAndGetRequest();

                RequestStats.getInstance().fpCount.incrementAndGet();

                Stable stableMsg = new Stable(msg.getView(), request);
                stableChannel.sendToAll(stableMsg);


            } else if (info.hasNack() && info.isClassicQuorum()) {

//                logger.debug("Duration2: {}", System.currentTimeMillis() - proposedReqs[id].sentTime);

                Request request = info.updateAndGetRequest();

                tsGenerator.setTimestamp(info.getMaxPosition());

                request.setPosition(tsGenerator.newTimestamp());

                retryReplies[id] = new RetryReplyInfo(request, numReplicas);

//                Stable stableMsg = new Stable(msg.getView(), request);
//                stableChannel.sendToAll(stableMsg);

                Retry retryMsg = new Retry(msg.getView(), request);
                proposeChannel.sendToAll(retryMsg);

            } else if (info.isClassicQuorum()) {

                Request request = info.updateAndGetRequest();

                info.setSlowProposeFuture(
                        intDispatcher.schedule(() -> sendSlowPropose(msg.getView(), request, info),
                                fpTimeout, TimeUnit.MILLISECONDS)
                );

//                logger.exit();
                return;

            } else {

//                logger.exit();
                return;

            }

            ScheduledFuture<?> future = info.getSlowProposeFuture();
            if (future != null) {
                future.cancel(false);
            }

            info.setDone();
        }

//        logger.exit();
    }

    private void sendSlowPropose(int view, Request request, FastProposeReplyInfo fastInfo) {
//        logger.entry(view, request, fastInfo);

        if (fastInfo != null) {
            synchronized (fastInfo) {
                if (fastInfo.isDone()) {
                    return;
                }
                fastInfo.setDone();
            }
        }

//        if (logger.isTraceEnabled())
//            logger.trace("Taking the slow phase for {}", request);

        SlowPropose proposeMsg = new SlowPropose(view, request);

        spReplies[getIntId(request.getId())] =
                new SlowProposeReplyInfo(request, ProcessDescriptor.getInstance().numReplicas);

        proposeChannel.sendToAll(proposeMsg);

//        logger.exit();
    }

    void onSlowPropose(SlowPropose msg, int sender) {
//        logger.entry(msg, sender);

        int view = msg.getView();

        Request msgRequest = msg.getRequest();
        RequestId rId = msgRequest.getId();
        int id = getIntId(rId);

        synchronized (reqInfos[id]) {
            RequestInfo reqInfo = reqInfos[id];

            if (reqInfo.getId() == null) {
                reqInfo.init(rId, view, RequestStatus.PreSlowPending);
            }

            if ((reqInfo.getView() > view) || (reqInfo.getView() == view
                    && reqInfo.getStatusOrdinal() > RequestStatus.PreSlowPending.ordinal())) {
//                if (logger.isDebugEnabled())
//                    logger.debug(ON_PROPOSE, "Disregarding {} {}", msg, reqInfo);

//                logger.exit();
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

//                if (logger.isDebugEnabled()) {
//                    logger.debug(ON_PROPOSE, "Rejected: {}; Wait Set: {}", request, waitReqs);
//                }

                sendSlowProposeReject(reqInfo, request, view, sender);
            } else {
                slowProposeResume(reqInfo, request, view, sender, waitReqs, 0);
            }
        }

//        logger.exit();
    }

    private void slowProposeResume(RequestInfo reqInfo, Request request, int view, int sender,
                                   SortedSet<Request> waitReqs, int startIdx) {
//        logger.entry(request, sender, waitReqs, startIdx);

        synchronized (reqInfo) {

            if ((reqInfo.getView() > view) || (reqInfo.getView() == view
                    && reqInfo.getStatusOrdinal() > RequestStatus.SlowPending.ordinal())) {
//                logger.exit();
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

//                            if (logger.isDebugEnabled()) {
//                                logger.debug("{} is going to wait for {}; WaitReqs {}", request,
//                                        req, waitReqs);
//                            }
                            prQ.add(new OnSlowProposeRunner(reqInfo, request, view, sender, waitReqs, index));
//                            logger.exit();
                            return;

                        } else if (!req.getPred().contains(request.getId())) {

//                            if (logger.isTraceEnabled())
//                                logger.trace("{} does not contain {}", req, request);

                            sendSlowProposeReject(reqInfo, request, view, sender);
//                            logger.exit();
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

//        logger.exit();
    }

    private void sendSlowProposeReject(RequestInfo reqInfo, Request request, int view, int sender) {
//        logger.entry(request, view, sender);

        reqInfo.setStatus(RequestStatus.Rejected);
        request.setStatus(RequestStatus.Rejected);

        long position = tsGenerator.newTimestamp();

        Set<RequestId> predSet = conflictDetector.computeNewPredFor(request, position, null);
        request.setHasWhitelist(false);

        SlowProposeReply replyMsg = new SlowProposeReply(view, request.getId(), SlowProposeReply.Status.NACK, predSet, position);
        repliesChannel.sendMessage(replyMsg, sender);

//        logger.exit();
    }

    void onSlowProposeReply(SlowProposeReply msg, int sender) {
//        logger.entry(msg, sender);

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

//                logger.exit();
                return;

            }

            info.setDone();
        }

//        logger.exit();
    }

    void onRetry(Retry msg, int sender) {
//        logger.entry(msg, sender);

//        long start = System.currentTimeMillis();

        Request msgRequest = msg.getRequest();
        RequestId rId = msgRequest.getId();
        int id = getIntId(rId);

        int view = msg.getView();

        RequestInfo reqInfo = reqInfos[id];

        synchronized (reqInfo) {

            if (reqInfo.getId() == null) {
                reqInfo.init(rId, view, RequestStatus.PreAccepted);
            }

            if ((reqInfo.getView() > view) || (reqInfo.getView() == view
                    && reqInfo.getStatusOrdinal() > RequestStatus.PreAccepted.ordinal())) {
//                logger.exit();
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
                prQ.clear();
            }

            RetryReply replyMsg = new RetryReply(view, rId, predSet);
            repliesChannel.sendMessage(replyMsg, sender);

//            request.onRetryDuration = (int) (System.currentTimeMillis() - start);
        }
//        logger.exit();
    }

    void onRetryReply(RetryReply msg, int sender) {
//        logger.entry(msg, sender);

        RetryReplyInfo info = retryReplies[getIntId(msg.getRequestId())];

        synchronized (info) {
            info.addReply(msg, sender);

            if (!info.isClassicQuorum() || info.isDone()) {
//                logger.exit();
                return;
            }

            RequestStats.getInstance().retryCount.getAndIncrement();
            info.setDone();
//            if (logger.isTraceEnabled()) {
//                logger.trace("sending stable for {}", info);
//            }
            Stable stableMsg = new Stable(msg.getView(), info.updateAndGetRequest());
            stableChannel.sendToAll(stableMsg);
        }

//        logger.exit();
    }

    void onStable(Stable msg, int sender) {
//        logger.entry(msg, sender);

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
//                logger.exit();
                return;
            }

//            if (logger.isDebugEnabled())
//                logger.debug(STABLE, "ReqInfo: {}", reqInfo);

            Request request = conflictDetector.updateRequest(msgRequest);

            Queue<Runnable> prQ = proposeRunnables[id];

            synchronized (prQ) {
                reqInfo.setStatus(RequestStatus.Stable);
                request.setStatus(RequestStatus.Stable);
                request.setHasWhitelist(false);

//                if(logger.isDebugEnabled()) {
//                    logger.debug("Stable: {} ({})", request.getId(), request.getPosition());
//                }

                prQ.forEach(intDispatcher::submit);
                prQ.clear();
            }

            deliver(request);
//            caesar.deliver(request);
        }

//        logger.exit();
    }

    private void deliver(Request request) {
//        logger.entry(request);


        deliverResume(request, 0);

        ConcurrentMap<RequestId, Runnable> postDelQ = deliverRunnables[getIntId(request.getId())];
        synchronized (postDelQ) {
//            if (logger.isDebugEnabled()) {
//                logger.debug("Releasing1 reqs waiting on {}: {}", request, postDelQ);
//            }
            postDelQ.forEach((predReq, predRunnable) -> intDispatcher.submit(predRunnable));
            postDelQ.clear();
        }

//        // TODO: There is a problem here if "id" is not present in conflictDetector.
//        // Next step fixes this...

//        ConcurrentMap<RequestId, Runnable> postDelQ = deliverRunnables[getIntId(request.getId())];

//        request.getPred().removeIf(id -> {
//            Request predReq = conflictDetector.getRequest(id);
//            boolean remove = predReq != null
//                    && predReq.getStatus().ordinal() >= RequestStatus.Stable.ordinal()
//                    && predReq.getPosition() > request.getPosition();
//            if(remove) {
//                Runnable predRunnable = postDelQ.remove(id);
//                if (predRunnable != null) {
//                    intDispatcher.submit(predRunnable);
//                }
//            }
//            return remove;
//        });

//        logger.exit();
    }

    //TODO: CHECK THE CORRECTNESS HERE. BreakLoop P1 is not here. FIXED I THINK NOW
    private void deliverResume(Request request, int startIdx) {
//        logger.entry(request, startIdx);

        int index = 0;
        Set<RequestId> predSet = request.getPred();
        ConcurrentMap<RequestId, Runnable> postDelQ = deliverRunnables[getIntId(request.getId())];

        for (RequestId predId : predSet) {
            if (index >= startIdx) {
                Request predReq = conflictDetector.getRequest(predId);

                ConcurrentMap<RequestId, Runnable> queue = deliverRunnables[getIntId(predId)];

                synchronized (queue) {
                    if (predReq == null || predReq.getStatus().ordinal() < RequestStatus.Stable.ordinal()) {

                        queue.put(request.getId(), new OnDeliverRunner(request, index));
                        if (logger.isDebugEnabled()) {
                            logger.debug("DeliverSleep1: {} going to sleep for {}; {}", request, predId, predReq);
                        }
                        return;

                    } else if (predReq.getPosition() < request.getPosition()) {

                        if (predReq.getPred().contains(request.getId())) {
                            Runnable predRunnable = postDelQ.remove(predId);
                            if (predRunnable != null) {
                                intDispatcher.submit(predRunnable);
                            }
                        }

                        if (predReq.getStatus().ordinal() != RequestStatus.Delivered.ordinal()) {

                            queue.put(request.getId(), new OnDeliverRunner(request, index));
                            if (logger.isDebugEnabled()) {
                                logger.debug("DeliverResume2: {} going to sleep for {}", request, predReq);
                            }
                            return;

                        }

                    } else if (predReq.getPosition() > request.getPosition()) {

                        index++;
                        continue;

                    } else {
                        logger.fatal("SHOULD NOT BE HERE {}", request);
                    }

                }
            }
            index++;
        }

        caesar.deliver(request);

//        logger.exit();
    }

    void onDelivery(Request request) {
//        logger.entry(request);
        request.setStatus(RequestStatus.Delivered);

        ConcurrentMap<RequestId, Runnable> postDelQ = deliverRunnables[getIntId(request.getId())];

        synchronized (postDelQ) {
//            if (logger.isDebugEnabled()) {
//                logger.debug("Releasing2 reqs waiting on {}: {}", request, postDelQ);
//            }
            postDelQ.forEach((predReq, predRunnable) -> intDispatcher.submit(predRunnable));
            postDelQ.clear();
        }

        if (logger.isDebugEnabled()) {
            logger.debug("Delivered {}", request);
        }
//        logger.exit();
    }

    void startRecovery(int nodeId) {
//        logger.entry(nodeId);
//        if (logger.isFatalEnabled())
//            logger.fatal(RECOVERY, "Trying to recovery {}", nodeId);

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

//        logger.exit();
    }

    private void recover(RequestInfo rInfo) {
//        logger.entry(rInfo);
        RequestId rId = rInfo.getId();


        synchronized (rInfo) {
            int newView = rInfo.getView() + 1;

            recoveryReplies[getIntId(rId)] = new RecoveryInfo(conflictDetector.getRequest(rId),
                    ProcessDescriptor.getInstance().numReplicas);

            Recovery recoverMsg = new Recovery(newView, rId);
            otherChannel.sendToAll(recoverMsg);
        }

//        logger.exit();
    }

    void onRecovery(Recovery msg, int sender) {
//        logger.entry(msg, sender);
//        if (logger.isFatalEnabled())
//            logger.fatal(RECOVERY, "Received onRecovery {} from {}", msg, sender);

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
//                if (logger.isFatalEnabled())
//                    logger.fatal(RECOVERY, "Returning {}; {}", view, reqInfo);
//                logger.exit();
                return;
            }

            Request request = conflictDetector.getRequest(rId);

//            if (logger.isFatalEnabled())
//                logger.fatal(RECOVERY, "OnRecovery for {}", request);

            RecoveryReply replyMsg = new RecoveryReply(view, rId, request);
            otherChannel.sendMessage(replyMsg, sender);
        }

//        logger.exit();
    }

    void onRecoveryReply(RecoveryReply msg, int sender) {
//        logger.entry(msg, sender);
//        if (logger.isFatalEnabled())
//            logger.fatal(RECOVERY, "Recovering {} from {}", msg, sender);

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

//                if (logger.isFatalEnabled()) {
//                    logger.fatal(RECOVERY, "Info for {}: {}", rId, info);
//                }

                reply = info.getReplyWithStatus(RequestStatus.Stable, false);
                if (reply == null)
                    reply = info.getReplyWithStatus(RequestStatus.Delivered, false);

                if (reply != null) {
                    Request newReq = new Request(rId, request.getObjectIds(), request.getPayload(),
                            reply.getPosition(), reply.getPred(), RequestStatus.Stable, view);

                    Stable stableMsg = new Stable(view, newReq);
//                    logger.fatal(RECOVERY, "Sending {}", stableMsg);
                    stableChannel.sendToAll(stableMsg);

//                    logger.exit();
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

//                    logger.exit();
                    return;
                }

                reply = info.getReplyWithStatus(RequestStatus.Rejected, false);

                if (reply != null) {
                    Request newReq = new Request(rId, request.getObjectIds(), request.getPayload(),
                            tsGenerator.newTimestamp(), null, RequestStatus.PreFastPending, view);

                    Set<RequestId> whiteList = null;

//                    logger.fatal(RECOVERY, "Sending fast propose {} {} {}", view, newReq, whiteList);
                    sendFastPropose(view, newReq, whiteList);

//                    logger.exit();
                    return;
                }

                reply = info.getReplyWithStatus(RequestStatus.SlowPending, false);

                if (reply != null) {
                    Request newReq = new Request(rId, request.getObjectIds(), request.getPayload(),
                            reply.getPosition(), reply.getPred(), RequestStatus.PreSlowPending, view);

//                    logger.fatal(RECOVERY, "Sending slow propose {} {}", view, newReq);
                    sendSlowPropose(view, newReq, null);

//                    logger.exit();
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

//        logger.exit();
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
//            logger.trace("Calling fastProposeResume for {} and waitReq {}", request, waitReqs);
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
//            logger.trace("Calling slowProposeResume for {} and waitReq {}", request, waitReqs);
            slowProposeResume(info, request, view, sender, waitReqs, index);
        }
    }

    private class OnDeliverRunner implements Runnable {

        private final Request request;
        private final int index;

        public OnDeliverRunner(Request request, int index) {
            this.request = request;
            this.index = index;
        }

        @Override
        public void run() {
//            logger.debug("Delivery: {} {}", request, index);
            deliverResume(request, index);
        }

        @Override
        public String toString() {
            return "OnDeliverRunner{" +
                    "request=" + request +
                    ", index=" + index +
                    '}';
        }
    }

}
