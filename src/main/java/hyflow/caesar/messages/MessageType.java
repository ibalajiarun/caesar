package hyflow.caesar.messages;

/**
 * Represents message type.
 */
public enum MessageType {
    Propose,
    ProposeReply,
    Retry,
    RetryReply,
    Stable,

    Alive,

    // Special markers used by the network implementation to raise callbacks
    // There are no classes with this messages types
    ANY, // any message
    SENT
    // sent messages
}
