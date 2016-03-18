package hyflow.service;

import hyflow.common.RequestId;
import hyflow.main.RequestProducer;
import org.junit.Test;

/**
 * Created by balajiarun on 3/16/16.
 */
public class ServiceTest {

    @Test
    public void testService() {
        RequestProducer producer = new RequestProducer();
        producer.createRequest(new RequestId(0, 1));
        producer.createRequest(new RequestId(0, 2));
    }

}