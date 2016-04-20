package hyflow.caesar.messages;

/**
 * Represents message type.
 */
public enum MessageType {
    FastPropose,
    FastProposeReply,

    SlowPropose,
    SlowProposeReply,

    Retry,
    RetryReply,

    Stable,

    Alive,

    Barrier,

    // Special markers used by the network implementation to raise callbacks
    // There are no classes with this messages types
    ANY, // any message
    SENT
    // sent messages
}
