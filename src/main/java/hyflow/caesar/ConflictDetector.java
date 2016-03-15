package hyflow.caesar;

import hyflow.common.Request;
import hyflow.common.RequestId;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;

/**
 * Created by balajiarun on 3/11/16.
 */
public class ConflictDetector {

    private static ConflictDetector instance = new ConflictDetector(1000);

    public static ConflictDetector getInstance() {
        return instance;
    }

    private final Map<RequestId, Request> requestMap;
    private final List<Request>[] objReqMap;

    public ConflictDetector(int numObjects) {
        requestMap = new HashMap<>(100000);
        objReqMap = new List[numObjects];
        for(List list: objReqMap) {
            list = new ArrayList<Request>(100000);
        }
    }

    public void putRequest(Request request) {
        requestMap.putIfAbsent(request.getRequestId(), request);
        for(int object : request.getObjectIds()) {
            objReqMap[object].add(request);
        }
    }

    public Request getRequest (RequestId rId) {
        if(requestMap.containsKey(rId)) {
            return requestMap.get(rId);
        }
        return null;
    }

    public void wait(Request request) {
        int[] objectIds = request.getObjectIds();
        for (int oId : objectIds) {
            objReqMap[oId].forEach((Request r) -> {
                while(r.shouldWait(request)) {
                    try {
                        r.wait();
                    } catch (InterruptedException e) {
                        logger.error("Request interrupted during wait.");
                    }
                }
            });
        }
        return;
    }

    public boolean noConflictFor(Request request) {
        int[] objectIds = request.getObjectIds();
        for (int oId : objectIds) {
            boolean conflict = objReqMap[oId].stream().anyMatch((Request r) ->
                r.conflictsWith(request)
            );
            if(conflict) return false;
        }
        return true;
    }

    public long findHighestPosition(Request request) {
        long maxPosition = -1;
        int[] objectIds = request.getObjectIds();
        for (int oId : objectIds) {
            maxPosition = Math.max(
                    objReqMap[oId].stream().max((Request r1, Request r2)->
                            (int) (r1.getPosition() - r2.getPosition())).get().getPosition()
                    , maxPosition);

        }
        return maxPosition;
    }

    private final Logger logger = LogManager.getLogger(ConflictDetector.class);
}
