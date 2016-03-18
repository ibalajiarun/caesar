package hyflow.common;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Created by balajiarun on 3/17/16.
 */
public class TestIdGenerator {

    @Test
    public void testIdGenerator() {
        SimpleIdGenerator generator = new SimpleIdGenerator(0, 100);
        assertEquals(generator.next(), 0);
        assertEquals(generator.next(), 100);
    }

    @Test
    public void testClientIdGeneration() {
        // 1000 clients/replica and 30 replicas
        int step = 1000;

        SimpleIdGenerator generator1 = new SimpleIdGenerator(0, step);
        assertEquals(generator1.next(), 0);

        SimpleIdGenerator generator2 = new SimpleIdGenerator(2, step);
        for (int i = 0; i < 1000; i++) {
            assertEquals(generator2.next(), i * 1000 + 2);
        }
    }

}
