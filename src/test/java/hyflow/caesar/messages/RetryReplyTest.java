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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * Created by balajiarun on 3/14/16.
 */
public class RetryReplyTest extends AbstractMessageTestCase<RetryReply> {

    private RetryReply reply;
    private RequestId requestId;
    private Set<RequestId> pred;

    @Before
    public void setUp() {
        requestId = new RequestId(0, 100);
        pred = new TreeSet<>();
        pred.add(new RequestId(0, 200));
        pred.add(new RequestId(0, 201));
        pred.add(new RequestId(0, 202));

        reply = new RetryReply(0, requestId, pred);
    }

    @Test
    public void shouldInitializeFields() {

        assertEquals(requestId, reply.getRequestId());
        assertThat(pred, is(reply.getPred()));
    }

    @Test
    public void testSerialization() throws IOException, ClassNotFoundException {
        verifySerialization(reply);

        byte[] bytes = reply.toByteArray();
        assertEquals(bytes.length, reply.byteSize());

        ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        DataInputStream dis = new DataInputStream(bis);

        MessageType type = MessageType.values()[dis.readByte()];
        RetryReply deserializedReply = new RetryReply(dis);

        assertEquals(MessageType.RetryReply, type);
        compare(reply, deserializedReply);
        assertThat(deserializedReply.getPred(), is(pred));
        assertEquals(0, dis.available());
    }


    @Test
    public void shouldReturnCorrectMessageType() {
        assertEquals(MessageType.RetryReply, reply.getType());
    }


    @Override
    protected void compare(RetryReply first, RetryReply second) {
        assertEquals(first.getRequestId(), second.getRequestId());
    }
}
