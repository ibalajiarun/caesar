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
public final class RetryReply extends Message {

    private final RequestId requestId;

    private Collection<RequestId> pred;
    private byte[] bytePred;
    private int predSize;

    public RetryReply(int view, RequestId rId, byte[] pred, int predSize) {
        super(view);
        this.requestId = rId;
        this.bytePred = pred;
        this.predSize = predSize;
    }

    public RetryReply(DataInputStream input) throws IOException {
        super(input);
        requestId = new RequestId(input);

        int length = input.readInt();
        pred = new TreeSet<>();
        while (--length >= 0)
            pred.add(new RequestId(input));
    }

    public RequestId getRequestId() {
        return requestId;
    }

    public Collection<RequestId> getPred() {
        return pred;
    }

    @Override
    public MessageType getType() {
        return MessageType.RetryReply;
    }

    @Override
    protected void write(ByteBuffer bb) {
        requestId.writeTo(bb);

        bb.putInt(predSize);
        bb.put(bytePred);
    }

    @Override
    public int byteSize() {
        return super.byteSize() + requestId.byteSize() + 4 + bytePred.length;
    }

    @Override
    public String toString() {
        return "RetryReply{" +
                "requestId=" + requestId +
                ", pred=" + pred +
                '}';
    }
}
