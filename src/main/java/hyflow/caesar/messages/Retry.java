package hyflow.caesar.messages;

import hyflow.common.Request;
import hyflow.common.RequestId;
import hyflow.common.RequestStatus;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public final class Retry extends Message {
    private static final long serialVersionUID = 1L;

    private final Request request;
    private final RequestId requestId;
    private final int[] objectIds;
    private final long position;
    private final byte[] payload;

    public Retry(int view, Request request) {
        super(view);
        this.request = request;
        this.requestId = request.getId();
        this.objectIds = request.getObjectIds();
        this.position = request.getPosition();
        this.payload = request.getPayload();
    }

    public Retry(DataInputStream input) throws IOException {
        super(input);
        this.requestId = new RequestId(input.readInt(), input.readInt());

        int length = input.readInt();
        this.objectIds = new int[length];
        for (int i=0;i<length;i++) {
            this.objectIds[i] = input.readInt();
        }

        this.position = input.readLong();
        this.payload = new byte[input.readInt()];
        input.readFully(payload);

        request = new Request(requestId, objectIds, payload);
        request.setPosition(position);
        request.setStatus(RequestStatus.Accepted);
    }

    public MessageType getType() {
        return MessageType.Retry;
    }

    public Request getRequest() {
        return request;
    }

    public int byteSize() {
        return super.byteSize() + requestId.byteSize() +
                4 + (4 * objectIds.length) +
                8 + 4 + payload.length;
    }

    public String toString() {
        return "Retry(" + super.toString() + ")";
    }

    protected void write(ByteBuffer bb) {
        request.getId().writeTo(bb);

        int[] oIds = request.getObjectIds();
        bb.putInt(oIds.length);
        for(int oId : oIds)
            bb.putInt(oId);

        bb.putLong(request.getPosition());
        bb.putInt(request.getPayload().length);
        bb.put(request.getPayload());
    }
}
