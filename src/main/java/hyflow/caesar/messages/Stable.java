package hyflow.caesar.messages;

import hyflow.common.Request;
import hyflow.common.RequestId;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public final class Stable extends Message {
    private static final long serialVersionUID = 1L;

    private final Request request;
    private final RequestId requestId;
    private final int[] objectIds;
    private final long position;
    private final byte[] payload;

    public Stable(Request request) {
        this.request = request;
        this.requestId = request.getRequestId();
        this.objectIds = request.getObjectIds();
        this.position = request.getPosition();
        this.payload = request.getPayload();
    }

    public Stable(DataInputStream input) throws IOException {
        super(input);
        requestId = new RequestId(input.readInt(), input.readInt());

        int length = input.readInt();
        objectIds = new int[length];
        for (int i=0;i<length;i++) {
            objectIds[i] = input.readInt();
        }

        position = input.readLong();
        payload = new byte[input.readInt()];
        input.readFully(payload);

        request = new Request(requestId, objectIds, payload);
        request.setPosition(position);
    }

    public MessageType getType() {
        return MessageType.Propose;
    }

    public Request getRequest() {
        return request;
    }

    public int byteSize() {
        return super.byteSize() + requestId.byteSize() + 4 +
                (4 * objectIds.length) + 8 + 4 + payload.length;
    }

    public String toString() {
        return "Stable(" + super.toString() + ")";
    }

    protected void write(ByteBuffer bb) {
        requestId.writeTo(bb);

        bb.putInt(objectIds.length);
        for(int oId : objectIds)
            bb.putInt(oId);

        bb.putLong(position);
        bb.putInt(payload.length);
        bb.put(payload);
    }
}
