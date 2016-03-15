package hyflow.caesar.messages;

import hyflow.common.RequestId;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by balajiarun on 3/12/16.
 */
public final class RetryReply extends Message {

    private final RequestId requestId;
    private final List<RequestId> pred;

    public RetryReply(RequestId rId, List<RequestId> pred) {
        this.requestId = rId;
        this.pred = pred;
    }

    public RetryReply(DataInputStream input) throws IOException {
        requestId = new RequestId(input);

        int length = input.readInt();
        pred = new ArrayList<>(length);
        while (--length >= 0)
            pred.add(new RequestId(input));
    }

    public RequestId getRequestId() {
        return requestId;
    }

    public List<RequestId> getPred() {
        return pred;
    }

    @Override
    public MessageType getType() {
        return MessageType.ProposeReply;
    }

    @Override
    protected void write(ByteBuffer bb) {
        requestId.writeTo(bb);

        bb.putInt(pred.size());
        for (RequestId rId : pred) {
            rId.writeTo(bb);
        }
    }

    @Override
    public int byteSize() {
        return super.byteSize() + requestId.byteSize() + 1 + 4 + (pred.size() * requestId.byteSize());
    }

    @Override
    public String toString() {
        return super.toString();
    }
}
