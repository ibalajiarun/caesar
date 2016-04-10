package hyflow.caesar.messages;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Class which objects are passed between nodes and which represent barrier
 * requests.
 *
 * @author Tadeusz Kobus
 * @author Maciej Kokocinski
 */
public class BarrierPackage extends Message {
    public final String barrierName;
    public final int n;

    public BarrierPackage(String barrierName, int n) {
        super(0);
        this.barrierName = barrierName;
        this.n = n;
    }

    public BarrierPackage(DataInputStream input) throws IOException {
        super(input);
        this.n = input.readInt();

        byte[] nameBytes = new byte[input.readInt()];
        input.readFully(nameBytes);

        this.barrierName = new String(nameBytes);
    }

    public int byteSize() {
        return super.byteSize() + 4 + 4 + barrierName.getBytes().length;
    }

    @Override
    public MessageType getType() {
        return MessageType.Barrier;
    }

    @Override
    protected void write(ByteBuffer bb) {
        bb.putInt(n);
        bb.putInt(barrierName.getBytes().length);
        bb.put(barrierName.getBytes());
    }
}

