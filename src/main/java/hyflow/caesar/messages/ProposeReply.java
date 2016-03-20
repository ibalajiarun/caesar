package hyflow.caesar.messages;

import hyflow.common.RequestId;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.TreeSet;

/**
 * Created by balajiarun on 3/12/16.
 */
public final class ProposeReply extends Message {

    private final RequestId requestId;
    private final Status status;
    private final Set<RequestId> pred;
    private final long maxPosition;

    public ProposeReply(RequestId rId, Status status, Set<RequestId> pred, long maxPosition) {
        this.requestId = rId;
        this.status = status;
        this.pred = pred;
        this.maxPosition = maxPosition;
    }

    public ProposeReply(DataInputStream input) throws IOException {
        super(input);
        requestId = new RequestId(input);
        status = Status.values()[input.readUnsignedByte()];

        if(status == Status.ACK) {
            int length = input.readInt();
            pred = new TreeSet<>();
            while (--length >= 0)
                pred.add(new RequestId(input));
            maxPosition = -1;
        } else {
            maxPosition = input.readLong();
            pred = null;
        }
    }

    public RequestId getRequestId() {
        return requestId;
    }

    public Set<RequestId> getPred() {
        return pred;
    }

    public Status getStatus() {
        return status;
    }

    public long getMaxPosition() {
        return maxPosition;
    }

    @Override
    public MessageType getType() {
        return MessageType.ProposeReply;
    }

    @Override
    protected void write(ByteBuffer bb) {
        requestId.writeTo(bb);
        bb.put((byte) status.ordinal());

        if (status == Status.ACK) {
            bb.putInt(pred.size());
            for (RequestId rId : pred) {
                rId.writeTo(bb);
            }
        } else {
            bb.putLong(maxPosition);
        }
    }

    @Override
    public int byteSize() {
        if (status == Status.ACK)
            return super.byteSize() + requestId.byteSize() + 1 + 4 + (pred.size() * requestId.byteSize());
        else
            return super.byteSize() + requestId.byteSize() + 1 + 8;
    }

    @Override
    public String toString() {
        return "ProposeReply:" + super.toString() + "-" + requestId.toString() + ";" + pred + ";" + status + ";" + maxPosition;
    }

    public enum Status {
        ACK, NACK
    }
}
