package hyflow.caesar;

import hyflow.common.Request;
import hyflow.common.RequestId;
import org.junit.Before;
import org.junit.Test;

import static junit.framework.Assert.assertNotNull;
import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.*;

/**
 * Created by balajiarun on 3/17/16.
 */
public class ConflictDetectorTest {

    private ConflictDetector detector;

    private RequestId rId = new RequestId(0, 1);
    private int[] oIds = new int[]{0, 1, 2};
    private byte[] payload = new byte[]{100};
    private Request request;

    @Before
    public void setUp() {
        detector = new ConflictDetector(1000);
        request = new Request(rId, oIds, payload);
        request.setPosition(100);
    }

    @Test
    public void testPutRequest() {
        detector.putRequest(request);
        compare(request, detector.getRequest(new RequestId(0, 1)));
    }

    @Test
    public void testNoRequest() {
        assertNull(detector.getRequest(new RequestId(10, 10)));
    }

    @Test
    public void testFindWaitRequestReturnNull() {
        assertNull(detector.findWaitRequest(request));
    }

    @Test
    public void testFindWaitRequestReturnNotNull() {
        Request r = new Request(new RequestId(10, 10), new int[]{1, 2, 3}, new byte[]{100});
        r.setPosition(101);
        detector.putRequest(r);
        assertNotNull(detector.findWaitRequest(request));
        Request ret = detector.findWaitRequest(request);
        assertEquals(r, ret);
    }

    @Test
    public void testNoConflictFor() {
        Request r1 = new Request(new RequestId(1, 2), oIds, payload);
        r1.setPosition(200);
        Request r2 = new Request(new RequestId(3, 2), oIds, payload);
        r2.setPosition(300);

        detector.putRequest(r1);
        detector.putRequest(r2);

        assertFalse(detector.noConflictFor(request));
    }

    protected void compare(Request first, Request second) {
        assertEquals(first, second);
        assertEquals(first.getPosition(), second.getPosition());
        assertArrayEquals(first.getObjectIds(), second.getObjectIds());
        assertArrayEquals(first.getPayload(), second.getPayload());
    }

}
