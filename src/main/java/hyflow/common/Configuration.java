package hyflow.common;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.*;

public final class Configuration {

    public static final int UDP_RECEIVE_BUFFER_SIZE = 64 * 1024;
    public static final int UDP_SEND_BUFFER_SIZE = 64 * 1024;

    private final static Logger logger = LogManager.getLogger(Configuration.class);

    private final List<PID> processes;
    private final Properties configuration = new Properties();

    public Configuration(String confFile) throws IOException {
        // Load property from file there is one
        InputStream fis = Paths.get(confFile).toUri().toURL().openStream();
        configuration.load(fis);
        fis.close();
        logger.info("Configuration loaded from file: " + confFile);

        this.processes = Collections.unmodifiableList(loadProcessList());
    }

    public Configuration(List<PID> processes) {
        this.processes = processes;
    }

    public int getN() {
        return processes.size();
    }

    public List<PID> getProcesses() {
        return processes;
    }

    public PID getProcess(int id) {
        return processes.get(id);
    }

    public boolean containsKey(String key) {
        return configuration.containsKey(key);
    }

    public int getIntProperty(String key, int defValue) {
        String str = configuration.getProperty(key);
        if (str == null) {
            logger.trace("Property not found: " + key + ". Using default value: " +
                        defValue);
            return defValue;
        }
        return Integer.parseInt(str);
    }

    /**
     * Returns a given property, converting the value to a boolean.
     *
     * @param key - the key identifying the property
     * @param defValue - the default value to use in case the key is not found.
     * @return the value of key property or defValue if key not found
     */
    public boolean getBooleanProperty(String key, boolean defValue) {
        String str = configuration.getProperty(key);
        if (str == null) {
            logger.trace("Property not found: " + key + ". Using default value: " + defValue);
            return defValue;
        }
        return Boolean.parseBoolean(str);
    }

    /**
     *
     * @param key - the key identifying the property
     * @param defValue - the default value to use in case the key is not found.
     *
     * @return the value of key property or defValue if key not found
     */
    public String getProperty(String key, String defValue) {
        String str = configuration.getProperty(key);
        if (str == null) {
            logger.trace("Property not found: " + key + ". Using default value: " + defValue);
            return defValue;
        }
        return str;
    }

    private List<PID> loadProcessList() {
        List<PID> processes = new ArrayList<PID>();
        int i = 0;
        while (true) {
            String line = configuration.getProperty("process." + i);
            if (line == null) {
                break;
            }
            StringTokenizer st = new StringTokenizer(line, ":");
            PID pid = new PID(i, st.nextToken(), Integer.parseInt(st.nextToken()), Integer.parseInt(st.nextToken()));
            processes.add(pid);
            logger.info(pid.toString());
            i++;
        }
        return processes;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder(256);
        sb.append("Processes:\n");
        for (PID p : processes) {
            sb.append("  ").append(p).append("\n");
        }
        sb.append("Properties:\n");
        Object[] keys = configuration.keySet().toArray();
        Arrays.sort(keys);
        for (Object key : keys) {
            sb.append("  ").append(key).append("=").append(configuration.get(key)).append("\n");
        }
        // Remove the trailing '\n'
        return sb.substring(0, sb.length()-1);
    }

    public double getDoubleProperty(String key, double defultValue) {
        String str = configuration.getProperty(key);
        if (str == null) {
            logger.trace("Property not found: " + key + ". Using default value: " + defultValue);
            return defultValue;
        }
        return Double.parseDouble(str);
    }

    public long getLongProperty(String key, long defultValue) {
        String str = configuration.getProperty(key);
        if (str == null) {
            logger.trace("Property not found: " + key + ". Using default value: " + defultValue);
            return defultValue;
        }
        return Long.parseLong(str);
    }
}
