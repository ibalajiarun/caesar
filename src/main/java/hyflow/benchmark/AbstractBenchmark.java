package hyflow.benchmark;

import hyflow.service.Service;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.Properties;

/**
 * Created by balajiarun on 3/30/16.
 */
public abstract class AbstractBenchmark implements Service {

    private final static Logger logger = LogManager.getLogger(AbstractBenchmark.class);
    protected final Properties configuration = new Properties();

    public AbstractBenchmark(String fileName) throws IOException {
        InputStream fis = Paths.get(fileName).toUri().toURL().openStream();
        configuration.load(fis);
        fis.close();
        logger.info("Configuration loaded from file: " + fileName);
    }

}
