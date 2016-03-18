package hyflow.caesar.messages;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;

public abstract class Message implements Serializable {
    private static final long serialVersionUID = 1L;
    private long sentTime;

    protected Message() {
        this(System.currentTimeMillis());
    }

    protected Message(long sentTime) {
        this.sentTime = sentTime;
    }

    protected Message(DataInputStream input) throws IOException {
        sentTime = input.readLong();
    }

    public long getSentTime() {
        return sentTime;
    }

    public void setSentTime() {
        this.sentTime = System.currentTimeMillis();
    }

    public int byteSize() {
        return 1 + 8;
    }

    public final byte[] toByteArray() {
        ByteBuffer bb = ByteBuffer.allocate(byteSize());
        bb.put((byte) getType().ordinal());
        bb.putLong(sentTime);
        write(bb);

        assert bb.remaining() == 0 : "Wrong sizes. Limit=" + bb.limit() + ",capacity=" +
                bb.capacity() + ",position=" + bb.position();

        return bb.array();
    }
    
    public final void writeTo(ByteBuffer bb) {
        bb.put((byte) getType().ordinal());
        bb.putLong(sentTime);
        write(bb);
    }

    public abstract MessageType getType();

    protected abstract void write(ByteBuffer bb);
}
