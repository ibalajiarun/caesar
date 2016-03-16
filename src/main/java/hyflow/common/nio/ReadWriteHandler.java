package hyflow.common.nio;

public interface ReadWriteHandler {
    void handleRead();

    void handleWrite();
}
