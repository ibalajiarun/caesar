package hyflow.main;

import hyflow.common.Request;
import hyflow.common.RequestId;
import hyflow.service.Service;

import java.util.Random;

/**
 * Created by balajiarun on 3/16/16.
 */
public class RequestProducer implements Service {

    public final Random random;

    public RequestProducer() {
        random = new Random();
    }

    @Override
    public Request createRequest(RequestId rId, boolean read) {
        byte[] payload = new byte[]{1, 2, 3, 4, 5};
        int[] objs = new int[2];
        objs[0] = random.nextInt(5);
        objs[1] = random.nextInt(5);
        return new Request(rId, objs, payload);
    }

    @Override
    public void executeRequest(Request request) {

    }
}
