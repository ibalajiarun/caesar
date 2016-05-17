package hyflow.caesar.messages;

import hyflow.common.Request;
import hyflow.common.RequestId;
import hyflow.common.RequestStatus;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * Created by balaji on 4/21/16.
 */
public class RecoveryReply extends Message {

    private final RequestId requestId;
    private final boolean valid;
    private int requestView;
    private long position;
    private Collection<RequestId> pred;
    private RequestStatus status;

    private boolean hasWhitelist;

    public RecoveryReply(int view, RequestId rId, Request request) {
        super(view);
        this.requestId = rId;
        if (request != null) {
            this.requestView = request.getView();
            this.position = request.getPosition();
            this.status = request.getStatus();
            this.pred = request.getPred();
            this.hasWhitelist = request.hasWhitelist();
            this.valid = true;
        } else {
            this.valid = false;
        }
    }

    RecoveryReply(DataInputStream input) throws IOException {
        super(input);
        requestId = new RequestId(input);
        valid = input.readUnsignedByte() != 0;
        if (valid) {
            requestView = input.readInt();
            position = input.readLong();
            status = RequestStatus.values()[input.readUnsignedByte()];

            int length = input.readInt();
            pred = new ConcurrentSkipListSet<>();
            while (--length >= 0)
                pred.add(new RequestId(input));

            hasWhitelist = input.readUnsignedByte() != 0;
        }
    }

    public RequestId getRequestId() {
        return requestId;
    }

    public int getRequestView() {
        return requestView;
    }

    public long getPosition() {
        return position;
    }

    public Collection<RequestId> getPred() {
        return pred;
    }

    public RequestStatus getStatus() {
        return status;
    }

    public boolean isValid() {
        return valid;
    }

    public boolean hasWhitelist() {
        return hasWhitelist;
    }

    @Override
    public MessageType getType() {
        return MessageType.RecoveryReply;
    }

    @Override
    public int byteSize() {
        if (!valid)
            return super.byteSize() + requestId.byteSize() + 1;
        else
            return super.byteSize() + requestId.byteSize() + 1 + 4 + 8 + 1 + 4
                    + pred.size() * requestId.byteSize() + 1;
    }

    @Override
    protected void write(ByteBuffer bb) {
        requestId.writeTo(bb);
        if (valid) {
            bb.put((byte) 1);
            bb.putInt(requestView);
            bb.putLong(position);
            bb.put((byte) status.ordinal());

            bb.putInt(pred.size());
            for (RequestId rId : pred) {
                rId.writeTo(bb);
            }

            bb.put((byte) (hasWhitelist ? 1 : 0));
        } else {
            bb.put((byte) 0);
        }
    }

    @Override
    public String toString() {
        return "RecoveryReply{" +
                super.toString() +
                "requestId=" + requestId +
                ", valid=" + valid +
                ", requestView=" + requestView +
                ", position=" + position +
                ", pred=" + pred +
                ", status=" + status +
                ", hasWhitelist=" + hasWhitelist +
                '}';
    }
}
