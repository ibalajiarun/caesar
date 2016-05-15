package hyflow.common;

import hyflow.caesar.FastProposeReplyInfo;

import java.util.Arrays;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * Request from the user which needs to be inserted in state machine. It can 
 * also be a transaction if this partial order is used to insert transactions
 * from clients.
 * 
 */
public final class Request implements Comparable<Request> {

    public RequestId requestId;
    public int[] objectIds;

    public byte[] payload;

    private Set<RequestId> pred;

    private long position;
    private RequestStatus status;
    private int view;

    private boolean hasWhitelist;

//    public long startWait;
//    public long waitDuration;
//    public int onProposeDuration;
//    public int onRetryDuration;
//    public long sentTime;

    public Request(RequestId requestId, int[] objectIds, byte[] payload) {
        this.requestId = requestId;
        this.objectIds = objectIds;
        this.payload = payload;
        this.status = RequestStatus.Waiting;
        this.pred = new ConcurrentSkipListSet<>();
        this.view = 0;
        this.position = -1;
    }

    public Request(RequestId requestId, int[] objectIds, byte[] payload,
                   long position, Set<RequestId> pred, RequestStatus status, int view) {
        this.requestId = requestId;
        this.objectIds = objectIds;
        this.payload = payload;
        this.position = position;
        this.pred = pred == null ? new ConcurrentSkipListSet<>() : pred;
        this.status = status;
        this.view = view;
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

    public boolean hasWhitelist() {
        return hasWhitelist;
    }

    public void setHasWhitelist(boolean hasWhitelist) {
        this.hasWhitelist = hasWhitelist;
    }

    public synchronized int getView() {
        return view;
    }

    public synchronized void setView(int view) {
        this.view = view;
    }

    public synchronized void updateWith(Request other) {
        view = other.view;
        pred = other.pred;
        position = other.position;
        status = other.status;
    }

    public synchronized void init(Request newReq) {
        requestId = newReq.requestId;
        objectIds = newReq.objectIds;
        payload = newReq.payload;
        view = newReq.view;
        pred = newReq.pred;
        position = newReq.position;
        status = newReq.status;
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
    public synchronized String toString() {
        return "Request{" +
                "requestId=" + requestId +
                ", objectIds=" + Arrays.toString(objectIds) +
//                ", payload=" + Arrays.toString(payload) +
//                ", pred=" + pred +
                ", position=" + position +
                ", status=" + status +
//                ", view=" + view +
                '}';
    }

}