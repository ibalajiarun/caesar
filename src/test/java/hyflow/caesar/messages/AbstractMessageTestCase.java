package hyflow.caesar.messages;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * Base test class for message tests.
 */
public abstract class AbstractMessageTestCase<T extends Message> {

    @SuppressWarnings("unchecked")
    protected void verifySerialization(T message) throws IOException, ClassNotFoundException {
        byte[] bytes = message.toByteArray();
        assertEquals(bytes.length, message.byteSize());

        T deserialized = (T) MessageFactory.readByteArray(bytes);
        compare(message, deserialized);

        DataInputStream stream = new DataInputStream(new ByteArrayInputStream(bytes));
        deserialized = (T) MessageFactory.create(stream);
        compare(message, deserialized);
    }

    protected abstract void compare(T first, T second);
}
