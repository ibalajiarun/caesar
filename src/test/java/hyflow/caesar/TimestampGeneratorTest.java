package hyflow.caesar;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Created by balajiarun on 3/24/16.
 */
public class TimestampGeneratorTest {

    private TimestampGenerator generator;

    @Test
    public void testGetTimestamp() {
        generator = new TimestampGenerator(4, 9);
        assertEquals(4, generator.newTimestamp());
        assertEquals(13, generator.newTimestamp());
    }

    @Test
    public void testUpdateTimestampACK() {
        generator = new TimestampGenerator(4, 9);
        generator.setTimestamp(15);
        assertEquals(22, generator.newTimestamp());
    }

    @Test
    public void testUpdateTimestampNACK() {
        generator = new TimestampGenerator(4, 9);
        generator.newTimestamp();
        generator.newTimestamp();
        generator.setTimestamp(3);
        assertEquals(22, generator.newTimestamp());
    }
}
