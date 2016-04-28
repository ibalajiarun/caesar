package hyflow.caesar;

import hyflow.common.Request;
import hyflow.common.RequestId;
import hyflow.common.RequestStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.Collectors;

/**
 * Created by balajiarun on 3/11/16.
 */
public class ConflictDetector {

    private final ConcurrentHashMap<RequestId, Request> requestMap;
    private final SortedSet<Request>[] objReqMap;
    private final Logger logger = LogManager.getLogger(ConflictDetector.class);

    @SuppressWarnings("unchecked")
    public ConflictDetector(int numObjects) {
        requestMap = new ConcurrentHashMap<>(1000000);
        objReqMap = new ConcurrentSkipListSet[numObjects];
        for (int i = 0; i < numObjects; i++) {
            objReqMap[i] = new ConcurrentSkipListSet<>();
        }
    }

    public Request updateRequest(Request newReq) {
        logger.entry(newReq);

        Request request = requestMap.putIfAbsent(newReq.getId(), newReq);
        request = request == null ? newReq : request;

        synchronized (request) {

            if (request != newReq) {
                request.updateWith(newReq);
            } else {
                for (int oId : request.getObjectIds()) {
                    objReqMap[oId].add(request);
                }
            }
        }

        return logger.exit(request);
    }

    public Request getRequest(RequestId rId) {
        return requestMap.get(rId);
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
