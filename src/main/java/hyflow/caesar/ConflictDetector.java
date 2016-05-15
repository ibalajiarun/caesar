package hyflow.caesar;

import hyflow.common.ProcessDescriptor;
import hyflow.common.Request;
import hyflow.common.RequestId;
import hyflow.common.RequestStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zmq.Req;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * Created by balajiarun on 3/11/16.
 */
public class ConflictDetector {

//    private final ConcurrentHashMap<RequestId, Request> requestMap;

    private final Request[] requestMap;

    //    private final ArrayList<Request>[] objReqMap;
    private final TreeMap<Long, Request>[] objReqMap;
    private final ReadWriteLock[] reqMapLock;

    //    private final Logger logger = LogManager.getLogger(ConflictDetector.class);
    private final int numReplicas;

    @SuppressWarnings("unchecked")
    public ConflictDetector(int numObjects) {
        int mapsize = ProcessDescriptor.getInstance().proposerMapSize;
        numReplicas = ProcessDescriptor.getInstance().numReplicas;
        requestMap = new Request[mapsize];
        for (int i = 0; i < mapsize; i++) {
            requestMap[i] = new Request(null, null, null);
        }

//        objReqMap = new ArrayList[numObjects];
        objReqMap = new TreeMap[numObjects];
        reqMapLock = new ReadWriteLock[numObjects];
        for (int i = 0; i < numObjects; i++) {
//            if(i == 0) {
//                objReqMap[i] = new ArrayList<>(mapsize);
            objReqMap[i] = new TreeMap<>();
//            } else {
//                objReqMap[i] = new ArrayList<>();
//            }
            reqMapLock[i] = new ReentrantReadWriteLock(true);
        }
    }

    public Request updateRequest(Request newReq) {
//        logger.entry(newReq);

        RequestId rId = newReq.getId();
//        assert rId.getClientId() < numReplicas : String.format("Client Id %d is more than 5", rId.getClientId());
        int id = rId.getClientId() + rId.getSeqNumber() * numReplicas;

        boolean insert = false, update = false;
        long oldPos = -1;
        Request request = requestMap[id];
        synchronized (requestMap[id]) {
            if (request.getId() == null) {
                request.init(newReq);
                insert = true;
            } else {
                if (request.getPosition() != newReq.getPosition()) {
                    oldPos = request.getPosition();
                    update = true;
                }
                request.updateWith(newReq);
            }
        }

        if (insert) {
            for (int oId : request.getObjectIds()) {
                reqMapLock[oId].writeLock().lock();
                objReqMap[oId].put(request.getPosition(), request);
                reqMapLock[oId].writeLock().unlock();
            }
        }
        if (update) {
            for (int oId : request.getObjectIds()) {
                reqMapLock[oId].writeLock().lock();
                objReqMap[oId].remove(oldPos);
                objReqMap[oId].put(request.getPosition(), request);
                reqMapLock[oId].writeLock().unlock();
            }
        }

        return request;
    }

    public Request getRequest(RequestId rId) {
        int id = rId.getClientId() + rId.getSeqNumber() * numReplicas;
        synchronized (requestMap[id]) {
            Request req = requestMap[id];
            if (req.getId() == null) {
                return null;
            }
            return req;
        }
    }

    private static class Reject {
        public boolean reject;
    }

    public boolean computeWaitSetOrReject(final Request request, final SortedSet<Request> waitSet) {
//        logger.entry(request, waitSet);

        int[] objectIds = request.getObjectIds();
        RequestId rId = request.getId();
        long position = request.getPosition();

        for (int oId : objectIds) {
            reqMapLock[oId].readLock().lock();

            Map<RequestStatus, List<Request>> statusMap =
                    objReqMap[oId].headMap(position).entrySet()
                            .parallelStream()
                            .map(Map.Entry::getValue)
                            .filter(r -> r.getPosition() > position && !r.getPred().contains(rId))
                            .collect(Collectors.groupingByConcurrent(Request::getStatus));

            reqMapLock[oId].readLock().unlock();

            if (statusMap.get(RequestStatus.Accepted) != null &&
                    statusMap.get(RequestStatus.Stable) != null) {
                return true;
            }

            List<Request> pReqs = statusMap.get(RequestStatus.FastPending);
            if (pReqs != null)
                waitSet.addAll(pReqs);

            List<Request> rReqs = statusMap.get(RequestStatus.Rejected);
            if (rReqs != null)
                waitSet.addAll(rReqs);


//            for(Request req : objReqMap[oId]) {
//
//                if(req.getPosition() > position && !req.getPred().contains(rId)) {
//
//                    if(req.getStatus().ordinal() >= RequestStatus.Accepted.ordinal()) {
//                        reqMapLock[oId].readLock().unlock();
//                        return true;
//                    } else {
//                        waitSet.add(req);
//                    }
//                }
//            }

//            for(Map.Entry<Long,Request> entry : objReqMap[oId].headMap(position).entrySet()) {

//            Reject r = new Reject();
//            r.reject = false;
//
//            objReqMap[oId].headMap(position).forEach((pos, req) -> {
//                if (req.getPosition() > position && !req.getPred().contains(rId)) {
//                    if (req.getStatus().ordinal() >= RequestStatus.Accepted.ordinal()) {
//                        r.reject = true;
//                    } else {
//                        waitSet.add(req);
//                    }
//                }
//            });
////            }
//
//            reqMapLock[oId].readLock().unlock();
//            return r.reject;
        }

//        return logger.exit(false); // Don't Reject yet.
        return false;
    }

    public SortedSet<RequestId> computeNewPredFor(Request request, long position, Set<RequestId> whiteList) {
//        logger.entry(request, position);

        SortedSet<RequestId> predSet = new ConcurrentSkipListSet<>();
        int[] objectIds = request.getObjectIds();

        for (int oId : objectIds) {
            reqMapLock[oId].readLock().lock();

//            for (Request req : objReqMap[oId]) {
//
//                RequestId reqId = req.getId();
//                if (reqId != request.getId()) {
//
//                    if (whiteList == null && req.getPosition() < position) {
//
//                        predSet.add(reqId);
//
//                    } else if (whiteList != null) {
//
//                        if(whiteList.contains(reqId) || (req.getPosition() < position
//                                && (req.getStatus() == RequestStatus.SlowPending
//                                || req.getStatus().ordinal() >= RequestStatus.Accepted.ordinal()))) {
//
//                            predSet.add(reqId);
//
//                        }
//
//                    }
//
//                }
//
//            }

            objReqMap[oId].tailMap(position).forEach((pos, req) -> {
                RequestId reqId = req.getId();
                if (reqId != request.getId()) {
                    if (whiteList == null && pos < position) {

                        predSet.add(reqId);

                    } else if (whiteList != null) {

                        if (whiteList.contains(reqId) || (pos < position
                                && (req.getStatus() == RequestStatus.SlowPending
                                || req.getStatus().ordinal() >= RequestStatus.Accepted.ordinal()))) {

                            predSet.add(reqId);
                        }
                    }
                }
            });

//            predSet = objReqMap[oId].parallelStream()
//                            .filter(r -> r.getId() != request.getId())
//                            .filter(r -> {
//                                if (whiteList != null) {
//                                    return whiteList.contains(r.getId()) || (r.getPosition() < position
//                                            && (r.getStatus() == RequestStatus.SlowPending
//                                            || r.getStatus().ordinal() >= RequestStatus.Accepted.ordinal()));
//                                } else {
//                                    return r.getPosition() < position && r.getStatus().ordinal() >= RequestStatus.Stable.ordinal();
//                                }
//                            })
//                            .map(Request::getId)
//                            .collect(Collectors.toCollection(ConcurrentSkipListSet::new));

            reqMapLock[oId].readLock().unlock();



        }


//        return logger.exit(predSet);
        return predSet;
    }
}
