package hyflow.caesar.messages;

import hyflow.common.Request;
import hyflow.common.RequestId;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

/**
 * Created by balajiarun on 3/14/16.
 */
public class ProposeReplyTest extends AbstractMessageTestCase<ProposeReply> {

    private ProposeReply reply;

    private Request request;
    private RequestId requestId;
    private Set<RequestId> pred;

    @Before
    public void setUp() {
        requestId = new RequestId(0, 100);
        pred = new TreeSet<>();
        pred.add(new RequestId(0, 200));
        pred.add(new RequestId(0, 201));
        pred.add(new RequestId(0, 202));

        request = new Request(requestId, new int[]{0, 1}, new byte[]{});
    }

    @Test
    public void shouldInitializeFields() {
        request.setPred(pred);
        reply = new ProposeReply(0, request, ProposeReply.Status.ACK);
        assertEquals(requestId, reply.getRequestId());
        assertThat(pred, is(reply.getPred()));
    }

    @Test
    public void testSerializationACK() throws IOException, ClassNotFoundException {
        request.setPred(pred);
        reply = new ProposeReply(0, request, ProposeReply.Status.ACK);

        verifySerialization(reply);

        byte[] bytes = reply.toByteArray();
        assertEquals(bytes.length, reply.byteSize());

        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        DataInputStream dis = new DataInputStream(bis);

        MessageType type = MessageType.values()[dis.readByte()];
        ProposeReply deserializedReply = new ProposeReply(dis);

        assertEquals(MessageType.ProposeReply, type);
        compare(reply, deserializedReply);
        assertThat(deserializedReply.getPred(), is(pred));
        assertEquals(0, deserializedReply.position());
        assertEquals(0, dis.available());
    }

    @Test
    public void testSerializationNACK() throws IOException, ClassNotFoundException {
        reply = new ProposeReply(0, request, ProposeReply.Status.NACK);

        verifySerialization(reply);

        byte[] bytes = reply.toByteArray();
        assertEquals(bytes.length, reply.byteSize());

        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        DataInputStream dis = new DataInputStream(bis);

        MessageType type = MessageType.values()[dis.readByte()];
        ProposeReply deserializedReply = new ProposeReply(dis);

        assertEquals(MessageType.ProposeReply, type);
        compare(reply, deserializedReply);
        assertTrue(deserializedReply.getPred().isEmpty());
        assertEquals(0, deserializedReply.position());
        assertEquals(0, dis.available());
    }

    @Test
    public void shouldReturnCorrectMessageType() {
        reply = new ProposeReply(0, request, ProposeReply.Status.ACK);
        assertEquals(MessageType.ProposeReply, reply.getType());
    }


    @Override
    protected void compare(ProposeReply first, ProposeReply second) {
        assertEquals(first.getRequestId(), second.getRequestId());
        assertEquals(first.getStatus(), second.getStatus());
    }
}
