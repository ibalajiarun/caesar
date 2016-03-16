package hyflow.common;

import java.util.ArrayList;
import java.util.List;

/**
 * Request from the user which needs to be inserted in state machine. It can 
 * also be a transaction if this partial order is used to insert transactions
 * from clients.
 * 
 * @see Reply
 */
public final class Request {

    public final RequestId requestId;
    public final int[] objectIds;

    private List<RequestId> pred;
    private long position;
    private RequestStatus status;

    public final byte[] payload;

    public Request(RequestId requestId, int[] objectIds, byte[] payload) {
        this.requestId = requestId;
        this.objectIds = objectIds;
        this.payload = payload;
        this.status = RequestStatus.Waiting;
        this.pred = new ArrayList<>();
    }

    public long getPosition() {
        return position;
    }

    public void setPosition(long position) {
        this.position = position;
    }

    public int[] getObjectIds() {
        return objectIds;
    }

    public RequestId getRequestId() {
        return requestId;
    }

    public byte[] getPayload() {
        return payload;
    }

    public RequestStatus getStatus() {
        return status;
    }

    public void setStatus(RequestStatus status) {
        this.status = status;
    }

    public List<RequestId> getPred() {
        return pred;
    }

    public boolean shouldWait(Request request) {
        if(request == null || request instanceof Request || request == this)
            return false;

        return position > request.position && !pred.contains(request)
                && status != RequestStatus.Stable && status != RequestStatus.Accepted;
    }

    @Override
    public boolean equals(Object other) {
        if(other == null || other instanceof Request)
            return false;

        if(other == this)
            return true;

        return ((Request) other).requestId == this.requestId;
    }

    public boolean conflictsWith(Request request) {
        return position > request.position && !pred.contains(request);
    }

}