package hyflow.caesar.messages;

import hyflow.common.Request;
import hyflow.common.RequestId;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

/**
 * Created by balajiarun on 3/14/16.
 */
public class ProposeReplyTest extends AbstractMessageTestCase<FastProposeReply> {

    private FastProposeReply reply;

    private Request request;
    private RequestId requestId;
    private Set<RequestId> pred;

    @Before
    public void setUp() {
        requestId = new RequestId(0, 100);
        pred = new ConcurrentSkipListSet<>();
        pred.add(new RequestId(0, 200));
        pred.add(new RequestId(0, 201));
        pred.add(new RequestId(0, 202));

        request = new Request(requestId, new int[]{0, 1}, new byte[]{});
    }

    @Test
    public void shouldInitializeFields() {
        request.setPred(pred);
        reply = new FastProposeReply(0, requestId, FastProposeReply.Status.ACK, pred, 10);
        assertEquals(requestId, reply.getRequestId());
        assertThat(pred, is(reply.getPred()));
    }

    @Test
    public void testSerializationACK() throws IOException, ClassNotFoundException {
        request.setPred(pred);
        reply = new FastProposeReply(0, requestId, FastProposeReply.Status.ACK, pred, 10);

        verifySerialization(reply);

        byte[] bytes = reply.toByteArray();
        assertEquals(bytes.length, reply.byteSize());

        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        DataInputStream dis = new DataInputStream(bis);

        MessageType type = MessageType.values()[dis.readByte()];
        FastProposeReply deserializedReply = new FastProposeReply(dis);

        assertEquals(MessageType.FastProposeReply, type);
        compare(reply, deserializedReply);
        assertThat(deserializedReply.getPred(), is(pred));
        assertEquals(10, deserializedReply.position());
        assertEquals(0, dis.available());
    }

    @Test
    public void testSerializationNACK() throws IOException, ClassNotFoundException {
        reply = new FastProposeReply(0, requestId, FastProposeReply.Status.NACK, pred, 11);

        verifySerialization(reply);

        byte[] bytes = reply.toByteArray();
        assertEquals(bytes.length, reply.byteSize());

        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        DataInputStream dis = new DataInputStream(bis);

        MessageType type = MessageType.values()[dis.readByte()];
        FastProposeReply deserializedReply = new FastProposeReply(dis);

        assertEquals(MessageType.FastProposeReply, type);
        compare(reply, deserializedReply);
        assertFalse(deserializedReply.getPred().isEmpty());
        assertEquals(11, deserializedReply.position());
        assertEquals(0, dis.available());
    }

    @Test
    public void shouldReturnCorrectMessageType() {
        reply = new FastProposeReply(0, requestId, FastProposeReply.Status.ACK, pred, 10);
        assertEquals(MessageType.FastProposeReply, reply.getType());
    }


    @Override
    protected void compare(FastProposeReply first, FastProposeReply second) {
        assertEquals(first.getRequestId(), second.getRequestId());
        assertEquals(first.getStatus(), second.getStatus());
    }
}
