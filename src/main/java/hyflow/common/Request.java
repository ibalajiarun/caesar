package hyflow.common;

import java.util.Set;
import java.util.TreeSet;

/**
 * Request from the user which needs to be inserted in state machine. It can 
 * also be a transaction if this partial order is used to insert transactions
 * from clients.
 * 
 */
public final class Request implements Comparable<Request> {

    public final RequestId requestId;
    public final int[] objectIds;
    public final byte[] payload;
    private Set<RequestId> pred;
    private long position;
    private RequestStatus status;

    public Request(RequestId requestId, int[] objectIds, byte[] payload) {
        this.requestId = requestId;
        this.objectIds = objectIds;
        this.payload = payload;
        this.status = RequestStatus.Waiting;
        this.pred = new TreeSet<>();
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

    public Set<RequestId> getPred() {
        return pred;
    }

    @Override
    public boolean equals(Object other) {
        if (other == null || !(other instanceof Request))
            return false;

        if(other == this)
            return true;

        return ((Request) other).requestId.equals(this.requestId);
    }

    @Override
    public int compareTo(Request o) {
        return requestId.compareTo(o.requestId);
    }

    @Override
    public String toString() {
        return String.format("Request(%s; status: %s)", requestId, status);
    }
}