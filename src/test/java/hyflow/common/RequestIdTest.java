package hyflow.common;

import org.junit.Test;

import static org.junit.Assert.*;

public class RequestIdTest {
    @Test
    public void shouldInitializeFields() {
        RequestId requestId = new RequestId(1, 2);
        assertEquals(1, (long) requestId.getClientId());
        assertEquals(2, requestId.getSeqNumber());
    }

    @Test
    public void shouldCompareTo() {
        RequestId first = new RequestId(1, 2);
        RequestId second = new RequestId(1, 3);

        assertTrue(first.compareTo(second) < 0);
        assertEquals(0, first.compareTo(first));
        assertTrue(second.compareTo(first) > 0);
    }

    @Test
    public void shouldCompareRequestsIdFromDifferentClients() {
        RequestId first = new RequestId(1, 2);
        RequestId second = new RequestId(2, 3);

        assertTrue(first.compareTo(second) < 0);
    }

    @Test
    public void shouldEqual() {
        RequestId requestId = new RequestId(1, 2);
        RequestId other = new RequestId(1, 2);

        assertFalse(requestId.equals(null));
        assertTrue(requestId.equals(requestId));
        assertTrue(requestId.equals(other));
        assertEquals(requestId.hashCode(), other.hashCode());
    }
}
