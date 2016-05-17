package hyflow.caesar;

import hyflow.common.ProcessDescriptor;
import hyflow.common.Request;
import hyflow.common.RequestId;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by balajiarun on 3/11/16.
 */
public class ConflictDetector {

//    private final ConcurrentHashMap<RequestId, Request> requestMap;

    private final Request[] requestMap;

    //    private final ArrayList<Request>[] objReqMap;
    private final TreeMap<Long, Request>[] objReqMap;
    private final TreeMap<Long, RequestId>[] objReqIdMap;

    private final ReadWriteLock[] reqMapLock;

    //    private final Logger logger = LogManager.getLogger(ConflictDetector.class);
    private final int numReplicas;

    @SuppressWarnings("unchecked")
    ConflictDetector(int numObjects) {
        int mapsize = ProcessDescriptor.getInstance().proposerMapSize;
        numReplicas = ProcessDescriptor.getInstance().numReplicas;
        requestMap = new Request[mapsize];
        for (int i = 0; i < mapsize; i++) {
            requestMap[i] = new Request(null, null, null);
        }

        objReqMap = new TreeMap[numObjects];
        objReqIdMap = new TreeMap[numObjects];
        reqMapLock = new ReadWriteLock[numObjects];
        for (int i = 0; i < numObjects; i++) {
            objReqMap[i] = new TreeMap<>();
            objReqIdMap[i] = new TreeMap<>();
            reqMapLock[i] = new ReentrantReadWriteLock(true);
        }
    }

    private int getIntId(RequestId rId) {
        return rId.getClientId() + (rId.getSeqNumber() * numReplicas);
    }

    Request updateRequest(Request newReq) {

        if (newReq.objectIds.length > 1) {
            throw new NotImplementedException();
        }

        RequestId rId = newReq.getId();
        int id = getIntId(rId);

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
                objReqIdMap[oId].put(request.getPosition(), request.getId());

                reqMapLock[oId].writeLock().unlock();
            }
        }
        if (update) {
            for (int oId : request.getObjectIds()) {
                reqMapLock[oId].writeLock().lock();

                objReqMap[oId].remove(oldPos);
                objReqIdMap[oId].remove(oldPos);

                objReqMap[oId].put(request.getPosition(), request);
                objReqIdMap[oId].put(request.getPosition(), request.getId());

                reqMapLock[oId].writeLock().unlock();
            }
        }

        return request;
    }

    public Request getRequest(RequestId rId) {
        int id = getIntId(rId);
        synchronized (requestMap[id]) {
            Request req = requestMap[id];
            if (req.getId() == null) {
                return null;
            }
            return req;
        }
    }

//    int computeWaitSetOrReject(final Request request, final SortedSet<Request> waitSet) {
//
//        int[] objectIds = request.getObjectIds();
//        RequestId rId = request.getId();
//        long position = request.getPosition();
//
//        long start = System.currentTimeMillis();
//        for (int oId : objectIds) {
//            reqMapLock[oId].readLock().lock();
//
////            int size = 0;
////            for(Map.Entry<Long, Request> entry : objReqMap[oId].tailMap(position, false).entrySet()) {
////                Request r = entry.getValue();
////                if(!r.getPred().contains(rId)) {
////                    if(r.getStatus().ordinal() >= RequestStatus.Accepted.ordinal()) {
////                        reqMapLock[oId].readLock().unlock();
////                        return -1;
////                    } else {
////                        waitSet.add(r);
//////                        size++;
//////                        if(size > 5) {
//////                            reqMapLock[oId].readLock().unlock();
//////                            waitSet.clear();
//////                            return 1;
//////                        }
////                    }
////                }
////            }
//
////            Map<RequestStatus, List<Request>> statusMap =
////                    objReqMap[oId].tailMap(position + 1).entrySet()
////                            .parallelStream()
////                            .map(Map.Entry::getValue)
////                            .filter(r -> !r.getPred().contains(rId))
////                            .collect(Collectors.groupingByConcurrent(Request::getStatus));
//
//
//            reqMapLock[oId].readLock().unlock();
//
////            if (statusMap.get(RequestStatus.Accepted) != null &&
////                    statusMap.get(RequestStatus.Stable) != null) {
////                return -1;
////            }
////
////            List<Request> pReqs = statusMap.get(RequestStatus.FastPending);
////            if (pReqs != null)
////                waitSet.addAll(pReqs);
////
////            List<Request> rReqs = statusMap.get(RequestStatus.Rejected);
////            if (rReqs != null)
////                waitSet.addAll(rReqs);
//
////            for(Request req : objReqMap[oId]) {
////
////                if(req.getPosition() > position && !req.getPred().contains(rId)) {
////
////                    if(req.getStatus().ordinal() >= RequestStatus.Accepted.ordinal()) {
////                        reqMapLock[oId].readLock().unlock();
////                        return true;
////                    } else {
////                        waitSet.add(req);
////                    }
////                }
////            }
//
////            for(Map.Entry<Long,Request> entry : objReqMap[oId].headMap(position).entrySet()) {
//
////            Reject r = new Reject();
////            r.reject = false;
////
////            objReqMap[oId].headMap(position).forEach((pos, req) -> {
////                if (req.getPosition() > position && !req.getPred().contains(rId)) {
////                    if (req.getStatus().ordinal() >= RequestStatus.Accepted.ordinal()) {
////                        r.reject = true;
////                    } else {
////                        waitSet.add(req);
////                    }
////                }
////            });
//////            }
////
////            reqMapLock[oId].readLock().unlock();
////            return r.reject;
//        }
//        long duration = System.currentTimeMillis() - start;
//        total += duration;
//        max = Math.max(duration, total);
//        count++;
//        if (count % 2000 == 0) {
//            System.out.println(String.format("AVG: %d MAX: %d", total / count, max));
//            total = 0;
//            count = 0;
//        }
//
//        return 0;
//    }

    Request[] computeWaitSet(final Request request) {
        int oId = request.objectIds[0];

        lock(oId);
        SortedMap<Long, Request> map = objReqMap[oId].tailMap(request.getPosition(), false);
        Request[] waitSet = new Request[map.size()];
        int index = 0;
        for (Request entry : map.values()) {
            waitSet[index++] = entry;
        }
        unlock(oId);

        return waitSet;
    }

    void lock(int oId) {
        reqMapLock[oId].readLock().lock();
    }

    void unlock(int oId) {
        reqMapLock[oId].readLock().unlock();
    }

    Collection<RequestId> computeNewPredFor(Request request, long position, Set<RequestId> whiteList) {
        int oId = request.objectIds[0];

        lock(oId);
        SortedMap<Long, RequestId> map = objReqIdMap[oId].headMap(position);
        unlock(oId);

//        for (int oId : objectIds) {
//            reqMapLock[oId].readLock().lock();
////            for (Request req : objReqMap[oId]) {
////
////                RequestId reqId = req.getId();
////                if (reqId != request.getId()) {
////
////                    if (whiteList == null && req.getPosition() < position) {
////
////                        predSet.add(reqId);
////
////                    } else if (whiteList != null) {
////
////                        if(whiteList.contains(reqId) || (req.getPosition() < position
////                                && (req.getStatus() == RequestStatus.SlowPending
////                                || req.getStatus().ordinal() >= RequestStatus.Accepted.ordinal()))) {
////
////                            predSet.add(reqId);
////
////                        }
////
////                    }
////
////                }
////
////            }
//
////            objReqMap[oId].headMap(position).forEach((pos, req) -> {
////                RequestId reqId = req.getId();
////                if (reqId != request.getId()) {
////                    if (whiteList == null && pos < position) {
////
////                        predSet.add(reqId);
////
////                    } else if (whiteList != null) {
////
////                        if (whiteList.contains(reqId) || (pos < position
////                                && (req.getStatus() == RequestStatus.SlowPending
////                                || req.getStatus().ordinal() >= RequestStatus.Accepted.ordinal()))) {
////
////                            predSet.add(reqId);
////                        }
////                    }
////                }
////            });
//            if(whiteList == null) {
//                predSet = objReqMap[oId].headMap(position);
//            } else {
//                throw new NotImplementedException();
//            }
////            predSet = objReqMap[oId].headMap(position).entrySet()
////                    .parallelStream()
////                    .map(Map.Entry::getValue)
////                    .filter(r -> {
////                        if(r.getId() != request.getId()) {
////                            if (whiteList == null) {
////                                return true;
////                            } else {
////                                return whiteList.contains(r.getId()) || (r.getPosition() < position
////                                        && (r.getStatus() == RequestStatus.SlowPending
////                                        || r.getStatus().ordinal() >= RequestStatus.Accepted.ordinal()));
////                            }
////                        }
////                        return false;
////                    })
////                    .map(Request::getId)
////                    .collect(Collectors.toCollection(ConcurrentSkipListSet::new));
//            reqMapLock[oId].readLock().unlock();
//
//        }
        return map.values();
    }
}
