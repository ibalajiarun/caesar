package hyflow.service;

import hyflow.common.Request;
import hyflow.common.RequestId;

/**
 * Created by balajiarun on 3/16/16.
 */
public interface Service {

    Request createRequest(RequestId rId);

}
