package hyflow.caesar.network;

import java.util.BitSet;

import hyflow.caesar.messages.Message;

public class MessageHandlerAdapter implements MessageHandler {

    public void onMessageReceived(Message msg, int sender) {
    }

    public void onMessageSent(Message message, BitSet destinations) {
    }
}
