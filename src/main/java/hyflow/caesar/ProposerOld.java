//package hyflow.caesar;
//
//import hyflow.caesar.messages.*;
//import hyflow.caesar.network.Network;
//import hyflow.common.*;
//import org.apache.logging.log4j.LogManager;
//import org.apache.logging.log4j.Logger;
//
//import java.util.Queue;
//import java.util.Set;
//import java.util.SortedSet;
//import java.util.TreeSet;
//import java.util.concurrent.ConcurrentHashMap;
//import java.util.concurrent.ConcurrentLinkedQueue;
//import java.util.concurrent.ConcurrentMap;
//import java.util.concurrent.LinkedBlockingQueue;
//
//class ProposerOld {
//
//    private final TimestampGenerator tsGenerator;
//    private final ConflictDetector conflictDetector;
//    private final Network network;
//    private final ThreadDispatcher dipatcher;
//    private final Caesar caesar;
//
//    private final ConcurrentMap<RequestId, FastProposeReplyInfo> proposalReplies;
//    private final ConcurrentMap<RequestId, RetryReplyInfo> retryReplies;
//
//    private final ConcurrentHashMap<RequestId, Queue<Runnable>> proposeRunnables;
//    private final ConcurrentHashMap<RequestId, Queue<Runnable>> deliverRunnables;
//
//    private final ConcurrentMap<RequestId, RequestInfo> ballots;
//
//    private final Logger logger = LogManager.getLogger(Proposer.class);
//    private final int localId;
//
//    ProposerOld(TimestampGenerator tsGenerator, ConflictDetector conflictDetector,
//             Network network, ThreadDispatcher dispatcher, Caesar caesar) {
//        this.tsGenerator = tsGenerator;
//        this.conflictDetector = conflictDetector;
//        this.network = network;
//        this.dipatcher = dispatcher;
//        this.caesar = caesar;
//
//        int mapSize = ProcessDescriptor.getInstance().proposerMapSize;
//        localId = ProcessDescriptor.getInstance().localId;
//
//        this.proposalReplies = new ConcurrentHashMap<>(mapSize);
//        this.retryReplies = new ConcurrentHashMap<>(mapSize);
//
//        this.proposeRunnables = new ConcurrentHashMap<>(mapSize);
//        this.deliverRunnables = new ConcurrentHashMap<>(mapSize);
//
//        this.ballots = new ConcurrentHashMap<>(mapSize);
//
////        QueueMonitor monitor = QueueMonitor.getInstance();
////        monitor.registerMap("proposalReplies", proposalReplies);
////        monitor.registerMap("retryReplies", retryReplies);
////        monitor.registerMap("proposeRunnables", proposeRunnables);
////        monitor.registerMap("deliverRunnables", deliverRunnables);
//    }
//
//    void fastPropose(Request request) {
//        request.setPosition(tsGenerator.newTimestamp());
//        request.setView(0);
//        sendPropose(0, request);
//    }
//
//    private void sendPropose(int view, Request request) {
//        FastPropose proposeMsg = new FastPropose(view, request);
//
//        proposalReplies.put(request.getId(),
//                new FastProposeReplyInfo(request, ProcessDescriptor.getInstance().numReplicas));
//
//        if(logger.isDebugEnabled()) {
//            logger.debug("Proposing");
//        }
//        network.sendToAll(proposeMsg);
//    }
//
//    void onPropose(FastPropose msg, int sender) {
//        Request msgRequest = msg.getRequest();
//        logger.debug("onPropose " + msgRequest);
//
//        RequestInfo newReqInfo = new RequestInfo(msg.getView(), RequestStatus.PrePending);
//
//        RequestInfo reqInfo = ballots.putIfAbsent(msgRequest.getId(), newReqInfo);
//        if (reqInfo == null)
//            reqInfo = newReqInfo;
//
//        synchronized (reqInfo) {
//
//            if (newReqInfo != reqInfo) {
//                if (reqInfo.getView() > msg.getView() ||
//                        reqInfo.getStatusOrdinal() > RequestStatus.PrePending.ordinal())
//                    return;
//            }
//
//            proposeRunnables.putIfAbsent(msgRequest.getId(), new ConcurrentLinkedQueue<>());
//
//            msgRequest.setStatus(RequestStatus.Pending);
//            Request request = conflictDetector.updateRequest(msgRequest, RequestStatus.Pending);
//
//            reqInfo.setStatus(RequestStatus.Pending);
//
//            SortedSet<Request> waitReqs = new TreeSet<>();
//            boolean shouldReject = conflictDetector.computeWaitSetOrReject(request, waitReqs);
//
//            assert !shouldReject : "OMG Reject";
////            assert waitReqs.size() == 0 : "OMG Req " + request + "Size: " + waitReqs;
//
//            if(waitReqs.size() > 0)
//                logger.fatal("Wait Reqs for " + request + "==" + waitReqs);
//
//            if (shouldReject) {
//                sendProposeReject(request, msg.getView(), sender);
//                reqInfo.setStatus(RequestStatus.Rejected);
//                return;
//            }
//
//            proposeResume(request, msg.getView(), sender, waitReqs, 0);
//        }
//    }
//
//    private void sendProposeReject(Request request, int view, int sender) {
//        logger.fatal("Sending reject for" + request);
//
//        request.setStatus(RequestStatus.Rejected);
//
//        long position = tsGenerator.newTimestamp();
//        request.setPosition(position);
//
//        Set<RequestId> predSet = conflictDetector.computeNewPredFor(request, position);
//        request.setPred(predSet);
//
//        FastProposeReply replyMsg = new FastProposeReply(view, request, FastProposeReply.Status.NACK);
//        network.sendMessage(replyMsg, sender);
//    }
//
//    private void proposeResume(Request request, int view, int sender, SortedSet<Request> waitReqs, int startIdx) {
//        int index = 0;
//        for (Request req : waitReqs) {
//            if(index >= startIdx) {
//                if (req.getStatus() != RequestStatus.Accepted && req.getStatus() != RequestStatus.Stable) {
//                    Queue<Runnable> prQ = proposeRunnables.get(req.getId());
//                    assert prQ != null : "FastPropose Runnable was not created for " + req.getId();
//                    synchronized (prQ) {
//                        if (req.getStatus() != RequestStatus.Accepted && req.getStatus() != RequestStatus.Stable) {
//                            prQ.add(new OnProposeRunner(request, view, sender, waitReqs, index));
//                        } else {
//                            continue;
//                        }
//                    }
//                    return;
//                } else if (!req.getPred().contains(request.getId())) {
//                    sendProposeReject(request, view, sender);
//                    return;
//                }
//            }
//            index++;
//        }
//
//
//        Set<RequestId> predSet = conflictDetector.computeNewPredFor(request, request.getPosition());
//        request.setPred(predSet);
//
//        FastProposeReply replyMsg = new FastProposeReply(0, request, FastProposeReply.Status.ACK);
//        network.sendMessage(replyMsg, sender);
//    }
//
//    void onProposeReply(FastProposeReply msg, int sender) {
//        logger.debug("proposeReply " + msg.getRequestId() + "-" + msg.getStatus());
//
//        RequestId rId = msg.getRequestId();
//        FastProposeReplyInfo info = proposalReplies.get(rId);
//
//        // TODO: CHECK IF THIS AFFECTS CORRECTNESS
//        if(info == null)
//            return;
////        assert info != null : "The info object is null for " + rId;
//
//        synchronized (info) {
//            if (info.isDone())
//                return;
//
//            info.addReply(msg, sender);
//
//            Request request = info.updateAndGetRequest();
//
//            if (!info.hasNack() && info.isFastQuorum()) {
//
//                Stable stableMsg = new Stable(msg.getView(), request);
//                logger.debug("stabling");
//
//                network.sendToAll(stableMsg);
//
//            } else if (info.hasNack() && info.isClassicQuorum()) {
//
//                logger.fatal("Retrying. Updating TS to " + info.getMaxPosition());
//                assert !info.hasNack() : "Retrying " + request;
//
//                tsGenerator.setTimestamp(info.getMaxPosition());
//
//                request.setStatus(RequestStatus.Rejected);
//                request.setPosition(tsGenerator.newTimestamp());
//
//                retryReplies.put(rId, new RetryReplyInfo(request, ProcessDescriptor.getInstance().numReplicas));
//
//                Retry retryMsg = new Retry(msg.getView(), request);
//                logger.fatal("retrying");
//                network.sendToAll(retryMsg);
//
//            } else {
//                return;
//            }
//            info.setDone();
//        }
//    }
//
//    void onStable(Stable msg, int sender) {
//        Request msgRequest = msg.getRequest();
//        RequestId rId = msgRequest.getId();
//
//        logger.debug("onstable " + msgRequest);
//
//        RequestInfo newReqInfo = new RequestInfo(msg.getView(), RequestStatus.Stable);
//        RequestInfo localInfo = ballots.putIfAbsent(rId, newReqInfo);
//        if (localInfo == null)
//            localInfo = newReqInfo;
//
//        synchronized (localInfo) {
//
//            if (newReqInfo != localInfo) {
//                if (localInfo.getView() > msg.getView() ||
//                        localInfo.getStatusOrdinal() > RequestStatus.Stable.ordinal()) {
//                    if(logger.isDebugEnabled())
//                        logger.debug("Returning due to info mismatch: "
//                            + localInfo + "; "
//                            + msg);
//                    return;
//                }
//
//            }
//
//            msgRequest.setStatus(RequestStatus.Stable);
//            assert msgRequest.getStatus() == RequestStatus.Stable : "Request is not stable";
//
//            Request request = conflictDetector.updateRequest(msgRequest, RequestStatus.Pending);
//            localInfo.setStatus(RequestStatus.Stable);
//            assert request.getStatus() == RequestStatus.Stable : "Req is not stable " + request;
//
//            Queue<Runnable> newPRQ = new ConcurrentLinkedQueue<>();
//            Queue<Runnable> prQ = proposeRunnables.putIfAbsent(rId, newPRQ);
//            if(prQ == null)
//                prQ = newPRQ;
////            assert prQ != null : "This queue should not be null here";
//            synchronized (prQ) {
//                prQ.forEach(dipatcher::submit);
//                prQ.clear();
//            }
//
//            if(logger.isDebugEnabled()) {
//                logger.debug("Request Stabilized " + request.toString());
//            }
//
////            dipatcher.submit(() -> deliver(request));
//            caesar.deliver(request, null);
//        }
//    }
//
//    private void deliver(Request request) {
//        Set<RequestId> predSet = request.getPred();
//
//        Queue<Runnable> newDeliverQ = new LinkedBlockingQueue<>();
//        Queue<Runnable> deliverQ = deliverRunnables.putIfAbsent(request.getId(), newDeliverQ);
//        if (deliverQ == null)
//            deliverQ = newDeliverQ;
//
//        // TODO: There is a problem here if "id" is not present in conflictDetector.
//        predSet.removeIf(id -> {
//            Request predReq = conflictDetector.getRequest(id);
//            return predReq != null &&
//                    (predReq.getStatus() == RequestStatus.Delivered
//                            || predReq.getStatus() == RequestStatus.Stable)
//                    && predReq.getPosition() > request.getPosition();
//        });
//
//        //TODO: CHECK THE CORRECTNESS HERE. BreakLoop P1 is not here. FIXED I THINK NOW
//        for(RequestId predId : predSet) {
//            Request predReq = conflictDetector.getRequest(predId);
//
//            if(predReq != null && predReq.getPosition() < request.getPosition()) {
//                predReq.getPred().remove(request.getId());
//            }
//
//            if (predReq == null || predReq.getStatus() != RequestStatus.Delivered) {
//                Queue<Runnable> newQ = new LinkedBlockingQueue<>();
//                Queue<Runnable> queue = deliverRunnables.putIfAbsent(predId, newQ);
//                if (queue == null)
//                    queue = newQ;
//
//                synchronized (queue) {
//                    if(predReq == null || predReq.getStatus() != RequestStatus.Delivered) {
//                        queue.add(new OnDeliverRunner(request));
//                    } else {
//                        dipatcher.execute(new OnDeliverRunner(request));
//                    }
//                }
//                return;
//            }
//        }
//
//        caesar.deliver(request, deliverQ);
//    }
//
//    public void onRetry(Retry msg, int sender) {
//        logger.debug("retry");
//
//        Request msgRequest = msg.getRequest();
//
////        Request request = conflictDetector.updateAndGetRequest(msgRequest.getId());
////
////        // TODO: Fix this. This is not the protocol.
////        assert request != null : "A retry without the fastPropose. Fix me!";
////
////        if (request != null) {
////            request.setPosition(msgRequest.getMaxPosition());
////            request.setPred(msgRequest.getPred());
////            request.setStatus(RequestStatus.Accepted);
////        } else {
////            logger.fatal("A retry without the fastPropose. Fix me.");
////        }
//
//        RequestInfo newReqInfo = new RequestInfo(msg.getView(), RequestStatus.Accepted);
//        RequestInfo localInfo = ballots.putIfAbsent(msgRequest.getId(), newReqInfo);
//        if (localInfo == null)
//            localInfo = newReqInfo;
//
//        synchronized (localInfo) {
//
//            if (newReqInfo != localInfo) {
//                if (localInfo.getView() > msg.getView() ||
//                        localInfo.getStatusOrdinal() > RequestStatus.Accepted.ordinal())
//                    return;
//            }
//
//            Request request = conflictDetector.updateRequest(msgRequest, RequestStatus.Pending);
//
//            SortedSet<RequestId> predSet = conflictDetector.computeNewPredFor(request, request.getPosition());
//
//            request.setStatus(RequestStatus.Accepted);
//
//            Queue<Runnable> newPRQ = new ConcurrentLinkedQueue<>();
//            Queue<Runnable> prQ = proposeRunnables.putIfAbsent(request.getId(), newPRQ);
//            if(prQ == null)
//                prQ = newPRQ;
//            assert prQ != null : "This queue should not be null here";
//            synchronized (prQ) {
//                prQ.forEach(dipatcher::submit);
//                prQ.clear();
//            }
//
//            RetryReply replyMsg = new RetryReply(0, request.getId(), request.getPred());
//            network.sendMessage(replyMsg, sender);
//        }
//    }
//
//    public void onRetryReply(RetryReply msg, int sender) {
//        logger.debug("retryreply");
//
////        RequestId rId = msg.getRequestId();
////        if(!retryReplies.containsKey(rId)) {
////            retryReplies.putIfAbsent(rId, new RetryReplyInfo(req));
////        }
////        Request req = conflictDetector.getRequest(rId);
//        RetryReplyInfo info = retryReplies.get(msg.getRequestId());
//
//        synchronized (info) {
//            info.addReply(msg, sender);
//
//            if (!info.isClassicQuorum() || info.isDone())
//                return;
//
//            info.setDone();
//            Stable stableMsg = new Stable(msg.getView(), info.updateAndGetRequest());
//            network.sendToAll(stableMsg);
//        }
//    }
//
//    public void onDelivery(Request request, Queue<Runnable> deliverQ) {
////        logger.fatal("Delivered");
//        request.setStatus(RequestStatus.Delivered);
//
////        assert deliverQ != null : "Why is delivery queue null here?";
////        if(logger.isDebugEnabled())
////            logger.debug("DeliveryQ Size: " + deliverQ);
////
////        synchronized (deliverQ) {
////            deliverQ.forEach(dipatcher::submit);
////            deliverQ.clear();
////        }
//    }
//
//    private class OnProposeRunner implements Runnable {
//
//        private final Request request;
//        private final int view;
//        private final int sender;
//        private final SortedSet<Request> waitReqs;
//        private final int index;
//
//        public OnProposeRunner(Request request, int view, int sender, SortedSet<Request> waitReqs, int index) {
//            this.request = request;
//            this.view = view;
//            this.sender = sender;
//            this.waitReqs = waitReqs;
//            this.index = index;
//        }
//
//        @Override
//        public void run() {
//            proposeResume(request, view, sender, waitReqs, index);
//        }
//    }
//
//    private class OnDeliverRunner implements Runnable {
//
//        private final Request request;
//
//        public OnDeliverRunner(Request request) {
//            this.request = request;
//        }
//
//        @Override
//        public void run() {
//            deliver(request);
//        }
//    }
//
//}