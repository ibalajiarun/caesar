package hyflow.main;

import hyflow.common.Request;

/**
 * Created by balaji on 5/6/16.
 */
public interface Client {
    void notifyClient(Request request);

    void run();
}
