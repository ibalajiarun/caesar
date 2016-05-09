package hyflow.caesar;

import hyflow.common.ProcessDescriptor;
import hyflow.common.Request;
import hyflow.common.RequestId;
import hyflow.common.RequestStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.Collectors;

/**
 * Created by balajiarun on 3/11/16.
 */
public class ConflictDetector {

//    private final ConcurrentHashMap<RequestId, Request> requestMap;

    private final Request[] requestMap;

    private final SortedSet<Request>[] objReqMap;
    private final Logger logger = LogManager.getLogger(ConflictDetector.class);
    private final int numReplicas;

    private long reqTotal = 0, reqMax = 0, reqCount;

    @SuppressWarnings("unchecked")
    public ConflictDetector(int numObjects) {
        int mapsize = ProcessDescriptor.getInstance().proposerMapSize;
        numReplicas = ProcessDescriptor.getInstance().numReplicas;
        requestMap = new Request[mapsize];
        for (int i = 0; i < mapsize; i++) {
            requestMap[i] = new Request(null, null, null);
        }

        objReqMap = new ConcurrentSkipListSet[numObjects];
        for (int i = 0; i < numObjects; i++) {
            objReqMap[i] = new ConcurrentSkipListSet<>();
        }
    }

    public Request updateRequest(Request newReq) {
        logger.entry(newReq);

//        long start = System.currentTimeMillis();

        RequestId rId = newReq.getId();
        assert rId.getClientId() < numReplicas : String.format("Client Id %d is more than 5", rId.getClientId());
        int id = rId.getClientId() + rId.getSeqNumber() * numReplicas;

//        Request request = requestMap.putIfAbsent(newReq.getId(), newReq);

//        long duration = (System.currentTimeMillis() - start);
//
//        reqTotal += duration;
//        reqMax = Math.max(duration, reqMax);
//        reqCount++;
//        if(reqCount % 5000 == 0) {
//            System.out.println(String.format("ReqMap: Avg: %f; Max %d ", reqTotal * 1.0 / reqCount, reqMax));
//            reqCount = 0;
//            reqTotal = 0;
//        }

        synchronized (requestMap[id]) {
            Request request = requestMap[id];
            if (request.getId() == null) {
                request.updateNewWith(newReq);
                for (int oId : request.getObjectIds()) {
                    objReqMap[oId].add(request);
                }
            } else {
                request.updateWith(newReq);
            }
            return logger.exit(request);
        }
    }

    public Request getRequest(RequestId rId) {
        int id = rId.getClientId() + rId.getSeqNumber();
        synchronized (requestMap[id]) {
            Request req = requestMap[id];
            if (req.getId() == null) {
                return null;
            }
            return req;
        }
    }

    public boolean computeWaitSetOrReject(final Request request, final SortedSet<Request> waitSet) {
        logger.entry(request, waitSet);

        int[] objectIds = request.getObjectIds();
        RequestId rId = request.getId();
        long position = request.getPosition();

        for (int oId : objectIds) {
            Map<RequestStatus, List<Request>> statusMap = objReqMap[oId]
                    .parallelStream()
                    .filter(r -> r.getPosition() > position && !r.getPred().contains(rId))
                    .collect(Collectors.groupingByConcurrent(Request::getStatus));

            if (statusMap.get(RequestStatus.Accepted) != null &&
                    statusMap.get(RequestStatus.Stable) != null) {
                return logger.exit(true); // Reject at once
            }

            List<Request> pReqs = statusMap.get(RequestStatus.FastPending);
            if (pReqs != null)
                waitSet.addAll(pReqs);

            List<Request> rReqs = statusMap.get(RequestStatus.Rejected);
            if (rReqs != null)
                waitSet.addAll(rReqs);
        }

        return logger.exit(false); // Don't Reject yet.
    }

    public SortedSet<RequestId> computeNewPredFor(Request request, long position, Set<RequestId> whiteList) {
        logger.entry(request, position);

        SortedSet<RequestId> predSet = new ConcurrentSkipListSet<>();
        int[] objectIds = request.getObjectIds();


        for (int oId : objectIds) {
            predSet.addAll(
                    objReqMap[oId].parallelStream()
                            .filter(r -> r.getId() != request.getId())
                            .filter(r -> {
                                if (whiteList != null) {
                                    return whiteList.contains(r.getId()) || (r.getPosition() < position
                                            && (r.getStatus() == RequestStatus.SlowPending
                                            || r.getStatus() == RequestStatus.Accepted
                                            || r.getStatus() == RequestStatus.Stable));
                                } else {
                                    return r.getPosition() < position;
                                }
                            })
                            .map(Request::getId)
                            .collect(Collectors.toSet())
            );
        }


        return logger.exit(predSet);
    }
}
