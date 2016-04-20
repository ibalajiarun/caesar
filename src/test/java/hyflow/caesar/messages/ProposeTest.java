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
 * Created by balajiarun on 3/16/16.
 */
public class ProposeTest extends AbstractMessageTestCase<FastPropose> {

    private RequestId rId = new RequestId(0, 1);
    private int[] oIds = new int[]{0, 1, 2};
    private byte[] payload = new byte[]{100};
    private FastPropose propose;
    private Request request;

    @Before
    public void setUp() {
        request = new Request(rId, oIds, payload);
        request.setPosition(100);
        propose = new FastPropose(0, request);
    }

    @Test
    public void shouldInitializeFields() {
        assertEquals(request, propose.getRequest());
        assertTrue(Arrays.equals(payload, propose.getRequest().getPayload()));
    }

    @Test
    public void testSerialization() throws IOException, ClassNotFoundException {
        verifySerialization(propose);

        byte[] bytes = propose.toByteArray();
        assertEquals(bytes.length, propose.byteSize());

        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        DataInputStream dis = new DataInputStream(bis);

        MessageType type = MessageType.values()[dis.readByte()];
        FastPropose deserializedPropose = new FastPropose(dis);

        assertEquals(MessageType.FastPropose, type);
        compare(propose, deserializedPropose);
        assertEquals(0, dis.available());
    }

    @Test
    public void shouldReturnCorrectMessageType() {
        assertEquals(MessageType.FastPropose, propose.getType());
    }

    @Override
    protected void compare(FastPropose first, FastPropose second) {
        assertEquals(first.getRequest(), second.getRequest());
        assertEquals(first.getRequest().getPosition(), second.getRequest().getPosition());
        assertArrayEquals(first.getRequest().getObjectIds(), second.getRequest().getObjectIds());
        assertArrayEquals(first.getRequest().getPayload(), second.getRequest().getPayload());
    }
}
