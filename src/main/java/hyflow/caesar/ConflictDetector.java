package hyflow.caesar;

import hyflow.common.Request;
import hyflow.common.RequestId;
import hyflow.common.RequestStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.Collectors;

/**
 * Created by balajiarun on 3/11/16.
 */
public class ConflictDetector {

    private final Map<RequestId, Request> requestMap;
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

    public void putRequest(Request request) {
        requestMap.put(request.getId(), request);
        for(int oId : request.getObjectIds()) {
            logger.debug("Putting objId " + oId + "from request " + request + " into objReqMap");
            objReqMap[oId].add(request);
        }
    }

    public Request getRequest(RequestId rId) {
        return requestMap.get(rId);
    }

    public boolean computeWaitSetOrReject(final Request request, SortedSet<Request> waitSet) {
        int[] objectIds = request.getObjectIds();
        RequestId rId = request.getId();
        long position = request.getPosition();

        for (int oId : objectIds) {
            Map<RequestStatus, List<Request>> statusMap = objReqMap[oId]
                    .parallelStream()
                    .filter(r -> r.getPosition() > position && !r.getPred().contains(rId))
                    .collect(Collectors.groupingByConcurrent(Request::getStatus));

            if (statusMap.get(RequestStatus.Accepted) != null ||
                    statusMap.get(RequestStatus.Stable) != null) {
                return false;
            }

            List<Request> pReqs = statusMap.get(RequestStatus.Pending);
            if (pReqs != null)
                waitSet.addAll(pReqs);

            List<Request> rReqs = statusMap.get(RequestStatus.Rejected);
            if (rReqs != null)
                waitSet.addAll(rReqs);
        }
        return true;
    }

//    public boolean noConflictFor(Request request) {
//        int[] objectIds = request.getObjectIds();
//        for (int oId : objectIds) {
//            boolean conflict = objReqMap[oId].stream().anyMatch((Request r) ->
//                    r.getPosition() > request.getPosition() && !r.getPred().contains(request.getId())
//            );
//            if(conflict) return false;
//        }
//        return true;
//    }

//    public long findHighestPosition(Request request) {
//        long maxPosition = -1;
//        int[] objectIds = request.getObjectIds();
//        for (int oId : objectIds) {
//            maxPosition = Math.max(
//                    objReqMap[oId].stream().max((Request r1, Request r2)->
//                            (int) (r1.getPosition() - r2.getPosition())).get().getPosition()
//                    , maxPosition);
//
//        }
//        assert maxPosition > -1 : "Invalid max position";
//        return maxPosition;
//    }
//
//    public void updatePredFor(Request request) {
//        for(int oId : request.getObjectIds()) {
//            for(Request r: objReqMap[oId]) {
//                request.getPred().add(r.getId());
//            }
//        }
//    }

    public SortedSet<RequestId> computeNewPredFor(Request request, long position) {
        SortedSet<RequestId> predSet = new TreeSet<>();
        int[] objectIds = request.getObjectIds();
        for (int oId : objectIds) {
            predSet.addAll(
                    objReqMap[oId].parallelStream()
                            .filter(r -> r.getPosition() < position)
                            .map(Request::getId)
                            .collect(Collectors.toList())
            );
        }
        return predSet;
    }
}
