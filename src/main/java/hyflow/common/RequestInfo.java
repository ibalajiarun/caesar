package hyflow.common;

/**
 * Created by balajiarun on 3/31/16.
 */
public class RequestInfo {

    private int view;
    private RequestStatus status;

    public RequestInfo(int view, RequestStatus status) {
        this.view = view;
        this.status = status;
    }

    public int getView() {
        return view;
    }

    public void setView(int view) {
        this.view = view;
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
                "view=" + view +
                ", status=" + status +
                '}';
    }
}
