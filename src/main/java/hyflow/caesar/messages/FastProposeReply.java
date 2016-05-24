package hyflow.caesar.messages;

import hyflow.common.RequestId;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * Created by balajiarun on 3/12/16.
 */
public final class FastProposeReply extends Message {

    private final RequestId requestId;
    private final Status status;
    private final long position;
    private final int waitTime;

    private int predSize;

    private Collection<RequestId> pred;
    private byte[] bytePred;

    public FastProposeReply(int view, RequestId rId, Status status, byte[] pred, int predSize, long position, int waitTime) {
        super(view);
        this.requestId = rId;
        this.status = status;
        this.bytePred = pred;
        this.predSize = predSize;
        this.position = position;
        this.waitTime = waitTime;
    }

    public FastProposeReply(DataInputStream input) throws IOException {
        super(input);
        requestId = new RequestId(input);
        status = Status.values()[input.readUnsignedByte()];

        int length = input.readInt();
        pred = new TreeSet<>();
        while (--length >= 0)
            pred.add(new RequestId(input));

        position = input.readLong();
        waitTime = input.readInt();
    }

    public RequestId getRequestId() {
        return requestId;
    }

    public Collection<RequestId> getPred() {
        return pred;
    }

    public Status getStatus() {
        return status;
    }

    public long position() {
        return position;
    }

    @Override
    public MessageType getType() {
        return MessageType.FastProposeReply;
    }

    @Override
    protected void write(ByteBuffer bb) {
        requestId.writeTo(bb);
        bb.put((byte) status.ordinal());

        bb.putInt(predSize);
        bb.put(bytePred);
        bb.putLong(position);
        bb.putInt(waitTime);
    }

    @Override
    public int byteSize() {
        return super.byteSize() + requestId.byteSize() + 1 + 4 + bytePred.length + 8 + 4;
    }

    @Override
    public String toString() {
        return "FastProposeReply{" +
                "requestId=" + requestId +
                ", status=" + status +
                ", pred=" + pred +
                ", position=" + position +
                '}';
    }

    public int getWaitTime() {
        return waitTime;
    }

    public enum Status {
        ACK, NACK, REPROPOSE
    }
}
