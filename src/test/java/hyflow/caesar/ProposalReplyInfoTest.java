package hyflow.caesar;

import hyflow.common.Request;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.mock;

/**
 * Created by balajiarun on 3/17/16.
 */
public class ProposalReplyInfoTest {

    private ProposalReplyInfo info;
    private Request request;

    @Before
    public void setUp() {
        request = mock(Request.class);
        info = new ProposalReplyInfo(request);
    }

    @Test
    public void testAddReply() {

    }

}
