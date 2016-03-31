package hyflow.common;

import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

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
    private int view;

    public Request(RequestId requestId, int[] objectIds, byte[] payload) {
        this.requestId = requestId;
        this.objectIds = objectIds;
        this.payload = payload;
        this.status = RequestStatus.Waiting;
        this.pred = new ConcurrentSkipListSet<>();
        this.view = 0;
    }

    public synchronized long getPosition() {
        return position;
    }

    public synchronized void setPosition(long position) {
        this.position = position;
    }

    public int[] getObjectIds() {
        return objectIds;
    }

    public RequestId getId() {
        return requestId;
    }

    public byte[] getPayload() {
        return payload;
    }

    public synchronized RequestStatus getStatus() {
        return status;
    }

    public synchronized void setStatus(RequestStatus status) {
        this.status = status;
    }

    public synchronized Set<RequestId> getPred() {
        return pred;
    }

    public synchronized void setPred(Set<RequestId> pred) {
        this.pred = pred;
    }

    public int getView() {
        return view;
    }

    public void setView(int view) {
        this.view = view;
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