package hyflow.caesar.messages;

import hyflow.common.Request;
import hyflow.common.RequestId;
import hyflow.common.RequestStatus;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.TreeSet;

public final class FastPropose extends Message {
    private static final long serialVersionUID = 1L;

    private final Request request;
    private final RequestId requestId;
    private final int[] objectIds;
    private final long position;
    private final byte[] payload;

    private final boolean valid;
    private final Set<RequestId> whiteList;

    public FastPropose(int view, Request request, Set<RequestId> whiteList) {
        super(view);
        this.request = request;
        this.requestId = request.getId();
        this.objectIds = request.getObjectIds();
        this.position = request.getPosition();
        this.payload = request.getPayload();

        this.whiteList = whiteList;
        if (whiteList != null) {
            this.valid = true;
        } else {
            this.valid = false;
        }

        this.request.setStatus(RequestStatus.FastPending);
    }

    public FastPropose(DataInputStream input) throws IOException {
        super(input);
        requestId = new RequestId(input);

        int length = input.readInt();
        objectIds = new int[length];
        for (int i=0;i<length;i++) {
            objectIds[i] = input.readInt();
        }

        position = input.readLong();
        payload = new byte[input.readInt()];
        input.readFully(payload);

        valid = input.readUnsignedByte() != 0;
        if (valid) {
            int wLength = input.readInt();

            whiteList = new TreeSet<>();
            while (--wLength >= 0)
                whiteList.add(new RequestId(input));
        } else {
            whiteList = null;
        }

        request = new Request(requestId, objectIds, payload, position, null, RequestStatus.FastPending, view);
    }

    public MessageType getType() {
        return MessageType.FastPropose;
    }

    public Request getRequest() {
        return request;
    }

    public Set<RequestId> getWhiteList() {
        return whiteList;
    }

    public int byteSize() {
        if (valid)
            return super.byteSize() + requestId.byteSize() + 4 + (4 * objectIds.length)
                    + 8 + 4 + payload.length + 1 + 4 + (requestId.byteSize() * whiteList.size());
        else
            return super.byteSize() + requestId.byteSize() + 4 +
                    (4 * objectIds.length) + 8 + 4 + payload.length + 1;

    }

    protected void write(ByteBuffer bb) {
        requestId.writeTo(bb);

        bb.putInt(objectIds.length);
        for(int oId : objectIds)
            bb.putInt(oId);

        bb.putLong(position);
        bb.putInt(payload.length);
        bb.put(payload);

        if (valid) {
            bb.put((byte) 1);
            bb.putInt(whiteList.size());
            for (RequestId rId : whiteList) {
                rId.writeTo(bb);
            }
        } else {
            bb.put((byte) 0);
        }
    }

    @Override
    public String toString() {
        return "FastPropose{" +
                "request=" + request +
                ", whiteList=" + whiteList +
                '}';
    }
}
