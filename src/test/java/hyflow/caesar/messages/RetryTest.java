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
 * Created by balajiarun on 3/20/16.
 */
public class RetryTest extends AbstractMessageTestCase<Retry> {

    private RequestId rId = new RequestId(0, 1);
    private int[] oIds = new int[]{0, 1, 2};
    private byte[] payload = new byte[]{100};
    private Retry retry;
    private Request request;

    @Before
    public void setUp() {
        request = new Request(rId, oIds, payload);
        request.setPosition(100);
        retry = new Retry(request);
    }

    @Test
    public void shouldInitializeFields() {
        assertEquals(request, retry.getRequest());
        assertTrue(Arrays.equals(payload, retry.getRequest().getPayload()));
    }

    @Test
    public void testSerialization() throws IOException, ClassNotFoundException {
        verifySerialization(retry);

        byte[] bytes = retry.toByteArray();
        assertEquals(bytes.length, retry.byteSize());

        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        DataInputStream dis = new DataInputStream(bis);

        MessageType type = MessageType.values()[dis.readByte()];
        Retry deserializedRetry = new Retry(dis);

        assertEquals(MessageType.Retry, type);
        compare(retry, deserializedRetry);
        assertEquals(0, dis.available());
    }

    @Test
    public void shouldReturnCorrectMessageType() {
        assertEquals(MessageType.Retry, retry.getType());
    }

    @Override
    protected void compare(Retry first, Retry second) {
        assertEquals(first.getRequest(), second.getRequest());
        assertArrayEquals(first.getRequest().getObjectIds(), second.getRequest().getObjectIds());
        assertArrayEquals(first.getRequest().getPayload(), second.getRequest().getPayload());
    }
}
