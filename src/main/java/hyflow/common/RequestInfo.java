package hyflow.common;

/**
 * Created by balajiarun on 3/31/16.
 */
public class RequestInfo {

    private final RequestId rId;
    private int view;
    private RequestStatus status;

    public RequestInfo(RequestId rId, int view, RequestStatus status) {
        this.rId = rId;
        this.view = view;
        this.status = status;
    }

    public RequestId getId() {
        return rId;
    }

    public int getView() {
        return view;
    }

    public void incrementView() {
        view += 1;
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

}
