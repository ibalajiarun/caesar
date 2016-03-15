package hyflow.caesar.messages;

import hyflow.common.Request;
import hyflow.common.RequestId;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public final class Retry extends Message {
    private static final long serialVersionUID = 1L;
    private final Request request;

    public Retry(Request request) {
        this.request = request;
    }

    public Retry(DataInputStream input) throws IOException {
        super(input);
        RequestId rId = new RequestId(input.readInt(), input.readInt());

        int length = input.readInt();
        int[] oIds = new int[length];
        for (int i=0;i<length;i++) {
            oIds[i] = input.readInt();
        }

        long position = input.readLong();
        byte[] payload = new byte[input.readInt()];
        input.readFully(payload);

        request = new Request(rId, oIds, payload);
        request.setPosition(position);
    }

    public MessageType getType() {
        return MessageType.Propose;
    }

    public Request getRequest() {
        return request;
    }

    public int byteSize() {
        return super.byteSize();
    }

    public String toString() {
        return "Propose(" + super.toString() + ")";
    }

    protected void write(ByteBuffer bb) {
        request.getRequestId().writeTo(bb);

        int[] oIds = request.getObjectIds();
        bb.putInt(oIds.length);
        for(int oId : oIds)
            bb.putInt(oId);

        bb.putLong(request.getPosition());
        bb.putInt(request.getPayload().length);
        bb.put(request.getPayload());
    }
}
