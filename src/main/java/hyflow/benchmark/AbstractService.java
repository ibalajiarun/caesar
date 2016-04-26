package hyflow.benchmark;

import hyflow.common.Request;
import hyflow.common.RequestId;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.Properties;

/**
 * Created by balajiarun on 3/30/16.
 */
public abstract class AbstractService {

    private final static Logger logger = LogManager.getLogger(AbstractService.class);
    protected final Properties configuration = new Properties();

    public AbstractService(String fileName) throws IOException {
        InputStream fis = Paths.get(fileName).toUri().toURL().openStream();
        configuration.load(fis);
        fis.close();
        logger.info("Configuration loaded from file: " + fileName);
    }

    public abstract Request createRequest(RequestId rId, boolean read, int requestType, int clientCount);

    public abstract void executeRequest(final Request request);

    public abstract int getTotalObjects();

}
