package hyflow.caesar.messages;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class Alive extends Message {
    private static final long serialVersionUID = 1L;

    public Alive() {
        super(0);
    }

    public Alive(DataInputStream input) throws IOException {
        super(input);
    }

    public MessageType getType() {
        return MessageType.Alive;
    }

    public int byteSize() {
        return super.byteSize();
    }

    public String toString() {
        return "ALIVE (" + super.toString() + ")";
    }

    protected void write(ByteBuffer bb) {
    }
}
