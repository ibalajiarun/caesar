package hyflow.common;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;

/**
 * Represents the unique id of request. To ensure uniqueness, the id contains
 * the id of client and the request sequence number. Every client should have
 * assigned unique client id, so that unique requests id can be created. Clients
 * gives consecutive sequence numbers to every sent request. The sequence number
 * starts with 0.
 */
public class RequestId implements Serializable, Comparable<RequestId> {
    private static final long serialVersionUID = 1L;

    private final int clientId;
    private final int seqNumber;

    /**
     * Creates new <code>RequestId</code> instance.
     * 
     * @param clientId - the id of client
     * @param seqNumber - the request sequence number
     */
    public RequestId(int clientId, int seqNumber) {
        this.clientId = clientId;
        this.seqNumber = seqNumber;
    }

    public RequestId(DataInputStream input) throws IOException {
        this.clientId = input.readInt();
        this.seqNumber = input.readInt();
    }

    /**
     * Returns the id of client.
     * 
     * @return the id of client
     */
    public int getClientId() {
        return clientId;
    }

    /**
     * Returns the request sequence number.
     * 
     * @return the request sequence number
     */
    public long getSeqNumber() {
        return seqNumber;
    }

    public int byteSize() {
        return 4+4;
    }

    public void writeTo(ByteBuffer bb) {
        bb.putInt(clientId);
        bb.putInt(seqNumber);
    }

    public int compareTo(RequestId requestId) {
        if (clientId != requestId.clientId) {
            throw new IllegalArgumentException("Cannot compare requests from different clients.");
        }
        return (int) (seqNumber - requestId.seqNumber);
    }

    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null || obj.getClass() != this.getClass()) {
            return false;
        }

        RequestId requestId = (RequestId) obj;
        return clientId == requestId.clientId && seqNumber == requestId.seqNumber;
    }

    public int hashCode() {
        int result = 17;
        result = 31*result + clientId;
        result = 31*result + seqNumber;
        return result;
    }

    public String toString() {
        return clientId + ":" + seqNumber;
    }
}
