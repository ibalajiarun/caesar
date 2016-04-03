package hyflow.caesar;

import hyflow.common.Request;
import hyflow.common.RequestId;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;

/**
 * Created by balajiarun on 3/17/16.
 */
public class ProposalReplyInfoTest {

    private ProposalReplyInfo info;
    private Request request;

    @Before
    public void setUp() {
        request = new Request(new RequestId(0, 1), new int[]{1, 2}, new byte[]{100});
        info = new ProposalReplyInfo(request, 1);
    }

    @Test
    public void testAddReply() {

    }

    @Test
    public void testShouldRetryCheckNull() {
        assertFalse(info.hasNack());
    }

    @Test
    public void testShouldRetryReturnTrue() {
//        ProposeReply reply = new ProposeReply(0, new RequestId(0, 10), ProposeReply.Status.NACK);
//        info.addReply(reply, 0);
//        assertTrue(info.hasNack());
    }

}
