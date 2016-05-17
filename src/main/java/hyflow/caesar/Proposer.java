package hyflow.caesar;

import hyflow.caesar.messages.*;
import hyflow.caesar.network.Network;
import hyflow.caesar.statistics.RequestStats;
import hyflow.common.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * Created by balajiarun on 4/9/16.
 */
public class Proposer {

    private static final Logger logger = LogManager.getLogger(Proposer.class);

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
        request.setPosition(tsGenerator.newTimestamp());
        request.setView(0);
        sendFastPropose(0, request, null);
    }

    private void sendFastPropose(int view, Request request, Set<RequestId> whiteList) {
        FastPropose proposeMsg = new FastPropose(view, request, whiteList);

        int id = getIntId(request.getId());
        fpReplies[id] = new FastProposeReplyInfo(request, ProcessDescriptor.getInstance().numReplicas);
        this.proposedReqs[id] = request;

        proposeChannel.sendToAll(proposeMsg);
    }

    void onFastPropose(FastPropose msg, int sender) {
        int view = msg.getView();

        Request msgRequest = msg.getRequest();
        RequestId rId = msgRequest.getId();

        tsGenerator.setTimestamp(msgRequest.getPosition());

        int id = getIntId(rId);
        synchronized (reqInfos[id]) {
            RequestInfo rInfo = reqInfos[id];

            if (rInfo.getId() == null) {
                rInfo.init(rId, view, RequestStatus.PreFastPending);
            }

            if ((rInfo.getView() > view) || (rInfo.getView() == view
                    && rInfo.getStatusOrdinal() > RequestStatus.FastPending.ordinal())) {
                return;
            }

            Collection<RequestId> predSet = conflictDetector.computeNewPredFor(msgRequest, msgRequest.getPosition(), msg.getWhiteList());
            msgRequest.setPred(predSet);
            Request request = conflictDetector.updateRequest(msgRequest);

            rInfo.setStatus(RequestStatus.FastPending);
            request.setStatus(RequestStatus.FastPending);

            assert request.getStatus() == RequestStatus.FastPending :
                    "Request not pending " + request + ";" + rInfo;

            Request[] waitSet = conflictDetector.computeWaitSet(request);

//            conflictDetector.lock(request.objectIds[0]);
//            int size = waitSet.size();
//            conflictDetector.unlock(request.objectIds[0]);
//
//            if(size > 10) {
//                FastProposeReply replyMsg = new FastProposeReply(view, request.getId(),
//                        FastProposeReply.Status.REPROPOSE, new byte[0], 0, request.getPosition());
//                repliesChannel.sendMessage(replyMsg, sender);
//                return;
//            }

            fastProposeResume(rInfo, request, view, sender, msg.getWhiteList(), waitSet, 0);

        }

    }

    private void fastProposeResume(RequestInfo reqInfo, Request request, int view, int sender,
                                   Set<RequestId> whiteList, Request[] waitReqs, int startIdx) {

        if (request.startWait > 0) {
            request.waitDuration += (int) (System.currentTimeMillis() - request.startWait);
        }

        RequestId rId = request.getId();

        synchronized (reqInfo) {

            if ((reqInfo.getView() > view) || (reqInfo.getView() == view
                    && reqInfo.getStatusOrdinal() > RequestStatus.FastPending.ordinal())) {
                return;
            }

            conflictDetector.lock(request.objectIds[0]);
            boolean reject = false;
            for (int index = startIdx; index < waitReqs.length; index++) {
                Request req = waitReqs[index];

                Queue<Runnable> prQ = proposeRunnables[getIntId(req.getId())];

                assert prQ != null : "prQ is null for " + request;

                synchronized (prQ) {

                    if (request.getPosition() < req.getPosition() && !req.getPred().contains(rId)) {

                        if (req.getStatus().ordinal() < RequestStatus.Accepted.ordinal()) {

                                prQ.add(new OnFastProposeRunner(reqInfo, request, view, sender, whiteList,
                                        waitReqs, index));
                            request.startWait = System.currentTimeMillis();
                            conflictDetector.unlock(request.objectIds[0]);
                                return;

                        } else {

                            reject = true;
                            sendFastProposeReject(reqInfo, view, sender, request);
                            conflictDetector.unlock(request.objectIds[0]);
                            return;

                        }
                    }
                }
            }
            conflictDetector.unlock(request.objectIds[0]);

            if (reject) {
                sendFastProposeReject(reqInfo, view, sender, request);
                return;
            }

            Collection<RequestId> predSet = conflictDetector.computeNewPredFor(request, request.getPosition(), whiteList);
            request.setPred(predSet);
            request.setHasWhitelist(whiteList != null);

            Queue<Runnable> prQ = proposeRunnables[getIntId(rId)];
            synchronized (prQ) {
                prQ.forEach(intDispatcher::submit);
                prQ.clear();
            }

            conflictDetector.lock(request.objectIds[0]);
            int predSize = predSet.size();
            ByteBuffer bb = ByteBuffer.allocate(predSize * rId.byteSize());
            for (RequestId r : predSet) {
                r.writeTo(bb);
            }
            conflictDetector.unlock(request.objectIds[0]);

            FastProposeReply replyMsg = new FastProposeReply(view, request.getId(),
                    FastProposeReply.Status.ACK, bb.array(), predSize, request.getPosition());
            repliesChannel.sendMessage(replyMsg, sender);
        }

    }

    private void sendFastProposeReject(RequestInfo reqInfo, int view, int sender, Request request) {

        request.setStatus(RequestStatus.Rejected);
        reqInfo.setStatus(RequestStatus.Rejected);

        long position = tsGenerator.newTimestamp();

        Collection<RequestId> predSet = conflictDetector.computeNewPredFor(request, position, null);
        request.setHasWhitelist(false);

        conflictDetector.lock(request.objectIds[0]);
        int predSize = predSet.size();
        ByteBuffer bb = ByteBuffer.allocate(predSize * request.getId().byteSize());
        for (RequestId r : predSet) {
            r.writeTo(bb);
        }
        conflictDetector.unlock(request.objectIds[0]);

        FastProposeReply replyMsg = new FastProposeReply(view, request.getId(),
                FastProposeReply.Status.NACK, bb.array(), predSize, position);
        repliesChannel.sendMessage(replyMsg, sender);

    }

    void onFastProposeReply(FastProposeReply msg, int sender) {

        RequestId rId = msg.getRequestId();
        int id = getIntId(rId);

        if (msg.getStatus() == FastProposeReply.Status.REPROPOSE) {
            fastPropose(proposedReqs[id]);
            return;
        }

        FastProposeReplyInfo info = fpReplies[id];

        // TODO: CHECK IF THIS AFFECTS CORRECTNESS
        if (info == null) {
            return;
        }

        synchronized (info) {
            if (info.isDone()) {
                return;
            }

            info.addReply(msg, sender);

            if (!info.hasNack() && info.isFastQuorum()) {

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

                request.startRetry = System.currentTimeMillis();

            } else if (info.isClassicQuorum()) {

                Request request = info.updateAndGetRequest();

                info.setSlowProposeFuture(
                        intDispatcher.schedule(() -> sendSlowPropose(msg.getView(), request, info),
                                fpTimeout, TimeUnit.MILLISECONDS)
                );

                return;

            } else {

                return;

            }

            ScheduledFuture<?> future = info.getSlowProposeFuture();
            if (future != null) {
                future.cancel(false);
            }

            info.setDone();
        }

    }

    private void sendSlowPropose(int view, Request request, FastProposeReplyInfo fastInfo) {

        if (fastInfo != null) {
            synchronized (fastInfo) {
                if (fastInfo.isDone()) {
                    return;
                }
                fastInfo.setDone();
            }
        }

        SlowPropose proposeMsg = new SlowPropose(view, request);

        spReplies[getIntId(request.getId())] =
                new SlowProposeReplyInfo(request, ProcessDescriptor.getInstance().numReplicas);

        proposeChannel.sendToAll(proposeMsg);

    }

    void onSlowPropose(SlowPropose msg, int sender) {

        throw new NotImplementedException();
//        int view = msg.getView();
//
//        Request msgRequest = msg.getRequest();
//        RequestId rId = msgRequest.getId();
//        int id = getIntId(rId);
//
//        synchronized (reqInfos[id]) {
//            RequestInfo reqInfo = reqInfos[id];
//
//            if (reqInfo.getId() == null) {
//                reqInfo.init(rId, view, RequestStatus.PreSlowPending);
//            }
//
//            if ((reqInfo.getView() > view) || (reqInfo.getView() == view
//                    && reqInfo.getStatusOrdinal() > RequestStatus.PreSlowPending.ordinal())) {
//                return;
//            }
//
////            proposeRunnables.computeIfAbsent(rId, k -> new ConcurrentLinkedQueue<>());
//
//            Request request = conflictDetector.updateRequest(msgRequest);
//
//            reqInfo.setStatus(RequestStatus.SlowPending);
//            request.setStatus(RequestStatus.SlowPending);
//
//            assert request.getStatus() == RequestStatus.SlowPending : "Request not pending " + request + ";" + reqInfo;
//
//            SortedSet<Request> waitReqs = new TreeSet<>();
//            int rejected = conflictDetector.computeWaitSetOrReject(request, waitReqs);
//
//            if (rejected == -1) {
//                sendSlowProposeReject(reqInfo, request, view, sender);
//            } else {
//                slowProposeResume(reqInfo, request, view, sender, waitReqs, 0);
//            }
//        }

    }

    private void slowProposeResume(RequestInfo reqInfo, Request request, int view, int sender,
                                   SortedSet<Request> waitReqs, int startIdx) {

        synchronized (reqInfo) {

            if ((reqInfo.getView() > view) || (reqInfo.getView() == view
                    && reqInfo.getStatusOrdinal() > RequestStatus.SlowPending.ordinal())) {
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

                            prQ.add(new OnSlowProposeRunner(reqInfo, request, view, sender, waitReqs, index));
                            return;

                        } else if (!req.getPred().contains(request)) {

                            sendSlowProposeReject(reqInfo, request, view, sender);
                            return;

                        }
                    }

                }
                index++;
            }

//            Set<RequestId> predSet = conflictDetector.computeNewPredFor(request, request.getPosition(), null);
//            request.setPred(predSet);
            request.setHasWhitelist(false);

//            SlowProposeReply replyMsg = new SlowProposeReply(0, request.getId(), SlowProposeReply.Status.ACK, predSet, request.getPosition());
//            repliesChannel.sendMessage(replyMsg, sender);
        }

    }

    private void sendSlowProposeReject(RequestInfo reqInfo, Request request, int view, int sender) {
        reqInfo.setStatus(RequestStatus.Rejected);
        request.setStatus(RequestStatus.Rejected);

        long position = tsGenerator.newTimestamp();

//        Set<RequestId> predSet = conflictDetector.computeNewPredFor(request, position, null);
//        request.setHasWhitelist(false);
//
//        SlowProposeReply replyMsg = new SlowProposeReply(view, request.getId(), SlowProposeReply.Status.NACK, predSet, position);
//        repliesChannel.sendMessage(replyMsg, sender);
    }

    void onSlowProposeReply(SlowProposeReply msg, int sender) {
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

                return;

            }

            info.setDone();
        }

    }

    void onRetry(Retry msg, int sender) {

        Request msgRequest = msg.getRequest();
        RequestId rId = msgRequest.getId();
        int id = getIntId(rId);

        tsGenerator.setTimestamp(msgRequest.getPosition());

        int view = msg.getView();

        RequestInfo reqInfo = reqInfos[id];

        synchronized (reqInfo) {

            if (reqInfo.getId() == null) {
                reqInfo.init(rId, view, RequestStatus.PreAccepted);
            }

            if ((reqInfo.getView() > view) || (reqInfo.getView() == view
                    && reqInfo.getStatusOrdinal() > RequestStatus.PreAccepted.ordinal())) {
                return;
            }

            Request request = conflictDetector.updateRequest(msgRequest);

            Collection<RequestId> newPredSet = conflictDetector.computeNewPredFor(request, request.getPosition(), null);

            conflictDetector.lock(request.objectIds[0]);
            int predSize = newPredSet.size() + request.getPred().size();
            ByteBuffer bb = ByteBuffer.allocate(predSize * rId.byteSize());
            for (RequestId r : newPredSet) {
                r.writeTo(bb);
            }
            for (RequestId r : request.getPred()) {
                r.writeTo(bb);
            }
            conflictDetector.unlock(request.objectIds[0]);

            request.setStatus(RequestStatus.Accepted);
            reqInfo.setStatus(RequestStatus.Accepted);

            request.setHasWhitelist(false);

            Queue<Runnable> prQ = proposeRunnables[id];

            synchronized (prQ) {
                prQ.forEach(intDispatcher::submit);
                prQ.clear();
            }

            RetryReply replyMsg = new RetryReply(view, rId, bb.array(), predSize);
            repliesChannel.sendMessage(replyMsg, sender);

        }
    }

    void onRetryReply(RetryReply msg, int sender) {

        int id = getIntId(msg.getRequestId());
        RetryReplyInfo info = retryReplies[id];

        if (info == null) {
            return;
        }

        synchronized (info) {
            info.addReply(msg, sender);

            if (!info.isClassicQuorum() || info.isDone()) {
                return;
            }

            RequestStats.getInstance().retryCount.getAndIncrement();
            info.setDone();

            Request request = info.updateAndGetRequest();
            request.retryDuration = (int) (System.currentTimeMillis() - request.startRetry);

            Stable stableMsg = new Stable(msg.getView(), request);
            stableChannel.sendToAll(stableMsg);
        }

    }

    void onStable(Stable msg, int sender) {

        Request msgRequest = msg.getRequest();
        RequestId rId = msgRequest.getId();
        int id = getIntId(rId);

        int view = msg.getView();

        tsGenerator.setTimestamp(msgRequest.getPosition());

        RequestInfo reqInfo = reqInfos[id];

        synchronized (reqInfo) {

            if (reqInfo.getId() == null) {
                reqInfo.init(rId, view, RequestStatus.PreStable);
            }

            if ((reqInfo.getView() > view) || (reqInfo.getView() == view
                    && reqInfo.getStatusOrdinal() > RequestStatus.PreStable.ordinal())) {
                return;
            }

            Request request = conflictDetector.updateRequest(msgRequest);

            Queue<Runnable> prQ = proposeRunnables[id];

            synchronized (prQ) {
                reqInfo.setStatus(RequestStatus.Stable);
                request.setStatus(RequestStatus.Stable);
                request.setHasWhitelist(false);

                prQ.forEach(intDispatcher::submit);
                prQ.clear();
            }

//            caesar.deliver(request);
            deliver(request);
        }

    }

    private void deliver(Request request) {

        request.startDeliver = System.currentTimeMillis();

        ConcurrentMap<RequestId, Runnable> postDelQ = deliverRunnables[getIntId(request.getId())];
        synchronized (postDelQ) {
            postDelQ.forEach((predReq, predRunnable) -> intDispatcher.submit(predRunnable));
            postDelQ.clear();
        }

        Collection<RequestId> pred = request.getPred();
        RequestId[] predArray = new RequestId[pred.size()];
        int index = 0;
        for (RequestId entry : pred) {
            predArray[index++] = entry;
        }

        deliverResume(request, predArray, 0);

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
    private void deliverResume(Request request, RequestId[] predArray, int startIdx) {

        ConcurrentMap<RequestId, Runnable> postDelQ = deliverRunnables[getIntId(request.getId())];

        for (int index = startIdx; index < predArray.length; index++) {
            if (index >= startIdx) {
                RequestId predId = predArray[index];
                Request predReq = conflictDetector.getRequest(predId);

                ConcurrentMap<RequestId, Runnable> queue = deliverRunnables[getIntId(predId)];

                synchronized (queue) {
                    if (predReq == null || predReq.getStatus().ordinal() < RequestStatus.Stable.ordinal()) {

                        queue.put(request.getId(), new OnDeliverRunner(request, predArray, index));
                        return;

                    } else if (predReq.getPosition() < request.getPosition()) {

                        if (predReq.getPred().contains(request.getId())) {
                            Runnable predRunnable = postDelQ.remove(predId);
                            if (predRunnable != null) {
                                intDispatcher.submit(predRunnable);
                            }
                        }

                        if (predReq.getStatus().ordinal() != RequestStatus.Delivered.ordinal()) {

                            queue.put(request.getId(), new OnDeliverRunner(request, predArray, index));
                            return;

                        }

                    } else if (predReq.getPosition() >= request.getPosition()) {

                        index++;
                        continue;

                    } else {
                        logger.fatal("SHOULD NOT BE HERE {} {}", request, predReq);
                    }

                }
            }
            index++;
        }

        caesar.deliver(request);

    }

    void onDelivery(Request request) {
        request.setStatus(RequestStatus.Delivered);

        ConcurrentMap<RequestId, Runnable> postDelQ = deliverRunnables[getIntId(request.getId())];

        synchronized (postDelQ) {
            postDelQ.forEach((predReq, predRunnable) -> intDispatcher.submit(predRunnable));
            postDelQ.clear();
        }

        request.deliverDuration = (int) (System.currentTimeMillis() - request.startDeliver);

        if (logger.isDebugEnabled()) {
            logger.debug("Delivered {}", request);
        }
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
        private final Request[] waitReqs;
        private final int index;
        private final int view;
        private final Set<RequestId> whiteList;

        public OnFastProposeRunner(RequestInfo info, Request request, int view, int sender, Set<RequestId> whiteList, Request[] waitReqs, int index) {
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
        private RequestId[] predArray;
        private final int index;

        public OnDeliverRunner(Request request, RequestId[] predArray, int index) {
            this.request = request;
            this.predArray = predArray;
            this.index = index;
        }

        @Override
        public void run() {
            deliverResume(request, predArray, index);
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
