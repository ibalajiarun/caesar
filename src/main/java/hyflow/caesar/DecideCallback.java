package hyflow.caesar;

import hyflow.common.Request;

import java.util.Queue;

/**
 * Created by balajiarun on 3/22/16.
 */
public interface DecideCallback {

    void deliver(Request requests, Queue<Runnable> deliverQ);

}
