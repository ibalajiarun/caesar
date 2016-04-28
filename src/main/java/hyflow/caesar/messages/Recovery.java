package hyflow.caesar.messages;

import hyflow.common.RequestId;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Created by balaji on 4/21/16.
 */
public class Recovery extends Message {

    private final RequestId requestId;

    public Recovery(int view, RequestId rId) {
        super(view);
        this.requestId = rId;
    }

    public Recovery(DataInputStream input) throws IOException {
        super(input);
        requestId = new RequestId(input);
    }

    public RequestId getRequestId() {
        return requestId;
    }

    @Override
    public MessageType getType() {
        return MessageType.Recovery;
    }

    @Override
    public int byteSize() {
        return super.byteSize() + requestId.byteSize();
    }

    @Override
    protected void write(ByteBuffer bb) {
        requestId.writeTo(bb);
    }

    @Override
    public String toString() {
        return "Recovery{" +
                "requestId=" + requestId +
                '}';
    }
}
