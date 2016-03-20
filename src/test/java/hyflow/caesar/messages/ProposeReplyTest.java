package hyflow.caesar.messages;

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
    private RequestId requestId;
    private Set<RequestId> pred;

    @Before
    public void setUp() {
        requestId = new RequestId(0, 100);
        pred = new TreeSet<>();
        pred.add(new RequestId(0, 200));
        pred.add(new RequestId(0, 201));
        pred.add(new RequestId(0, 202));
    }

    @Test
    public void shouldInitializeFields() {
        reply = new ProposeReply(requestId, ProposeReply.Status.ACK, pred, 100);

        assertEquals(requestId, reply.getRequestId());
        assertThat(pred, is(reply.getPred()));
    }

    @Test
    public void testSerializationACK() throws IOException, ClassNotFoundException {
        reply = new ProposeReply(requestId, ProposeReply.Status.ACK, pred, 100);

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
        assertEquals(deserializedReply.getMaxPosition(), -1);
        assertEquals(0, dis.available());
    }

    @Test
    public void testSerializationNACK() throws IOException, ClassNotFoundException {
        reply = new ProposeReply(requestId, ProposeReply.Status.NACK, pred, 100);

        verifySerialization(reply);

        byte[] bytes = reply.toByteArray();
        assertEquals(bytes.length, reply.byteSize());

        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        DataInputStream dis = new DataInputStream(bis);

        MessageType type = MessageType.values()[dis.readByte()];
        ProposeReply deserializedReply = new ProposeReply(dis);

        assertEquals(MessageType.ProposeReply, type);
        compare(reply, deserializedReply);
        assertNull(deserializedReply.getPred());
        assertEquals(deserializedReply.getMaxPosition(), 100);
        assertEquals(0, dis.available());
    }

    @Test
    public void shouldReturnCorrectMessageType() {
        reply = new ProposeReply(requestId, ProposeReply.Status.ACK, pred, 100);
        assertEquals(MessageType.ProposeReply, reply.getType());
    }


    @Override
    protected void compare(ProposeReply first, ProposeReply second) {
        assertEquals(first.getRequestId(), second.getRequestId());
        assertEquals(first.getStatus(), second.getStatus());
    }
}
