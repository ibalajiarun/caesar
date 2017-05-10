package hyflow.common;

/**
 * Created by balajiarun on 3/31/16.
 */
public class RequestInfo {

    private RequestId rId;
    private int view;
    private RequestStatus status;
    private boolean specRejected;
    private boolean fpDone;

    public RequestInfo() {
        this.rId = null;
        this.view = -1;
        this.status = RequestStatus.Waiting;
        this.specRejected = false;
        this.fpDone = false;
    }

    public void init(RequestId rId, int view, RequestStatus status) {
        this.rId = rId;
        this.view = view;
        this.status = status;
        this.specRejected = false;
//        this.fpDone = false;
    }

    public RequestId getId() {
        return rId;
    }

    public int getView() {
        return view;
    }

    public RequestStatus getStatus() {
        return status;
    }

    public void setStatus(RequestStatus status) {
        this.status = status;
    }

    public int getStatusOrdinal() {
        return status.ordinal();
    }

    @Override
    public String toString() {
        return "RequestInfo{" +
                "rId=" + rId +
                ", view=" + view +
                ", status=" + status +
                '}';
    }

    public boolean getSpecReject() {
        return specRejected;
    }

    public void setSpecReject() {
        specRejected = true;
    }

    int count = 0;
    public boolean isFPDone() {
        count++;
        if (count > 5) System.exit(-1);
        return fpDone;
    }

    public void setFPDone() {
        fpDone = true;
    }
}
