package hyflow.caesar.messages;

import hyflow.common.Request;
import hyflow.common.RequestId;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Arrays;

import static org.junit.Assert.*;

/**
 * Created by balajiarun on 3/18/16.
 */
public class StableTest extends AbstractMessageTestCase<Stable> {

    private RequestId rId = new RequestId(0, 1);
    private int[] oIds = new int[]{0, 1, 2};
    private byte[] payload = new byte[]{100};
    private Stable stable;
    private Request request;

    @Before
    public void setUp() {
        request = new Request(rId, oIds, payload);
        request.setPosition(100);
        stable = new Stable(0, request);
    }

    @Test
    public void shouldInitializeFields() {
        assertEquals(request, stable.getRequest());
        assertTrue(Arrays.equals(payload, stable.getRequest().getPayload()));
    }

    @Test
    public void testSerialization() throws IOException, ClassNotFoundException {
        verifySerialization(stable);

        byte[] bytes = stable.toByteArray();
        assertEquals(bytes.length, stable.byteSize());

        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        DataInputStream dis = new DataInputStream(bis);

        MessageType type = MessageType.values()[dis.readByte()];
        Stable deserializedStable = new Stable(dis);

        assertEquals(MessageType.Stable, type);
        compare(stable, deserializedStable);
        assertEquals(0, dis.available());
    }

    @Test
    public void shouldReturnCorrectMessageType() {
        assertEquals(MessageType.Stable, stable.getType());
    }

    @Override
    protected void compare(Stable first, Stable second) {
        assertEquals(first.getRequest(), second.getRequest());
        assertArrayEquals(first.getRequest().getObjectIds(), second.getRequest().getObjectIds());
        assertArrayEquals(first.getRequest().getPayload(), second.getRequest().getPayload());
    }

}
