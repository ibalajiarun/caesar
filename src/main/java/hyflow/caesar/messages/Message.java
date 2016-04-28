package hyflow.caesar.messages;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;

public abstract class Message implements Serializable {
    private static final long serialVersionUID = 1L;
    protected final int view;
    private long sentTime;

    protected Message(int view) {
        this(view, System.currentTimeMillis());
    }

    protected Message(int view, long sentTime) {
        this.view = view;
        this.sentTime = sentTime;
    }

    protected Message(DataInputStream input) throws IOException {
        view = input.readInt();
        sentTime = input.readLong();
    }

    public int getView() {
        return view;
    }

    public long getSentTime() {
        return sentTime;
    }

    public void setSentTime() {
        this.sentTime = System.currentTimeMillis();
    }

    public int byteSize() {
        return 1 + 4 + 8;
    }

    public final byte[] toByteArray() {
        ByteBuffer bb = ByteBuffer.allocate(byteSize());
        bb.put((byte) getType().ordinal());
        bb.putInt(view);
        bb.putLong(sentTime);
        write(bb);

        assert bb.remaining() == 0 : "Wrong sizes. Limit=" + bb.limit() + ",capacity=" +
                bb.capacity() + ",position=" + bb.position();

        return bb.array();
    }

    public abstract MessageType getType();

    protected abstract void write(ByteBuffer bb);

    @Override
    public String toString() {
        return "Message{" +
                "view=" + view +
                ", sentTime=" + sentTime +
                '}';
    }
}
