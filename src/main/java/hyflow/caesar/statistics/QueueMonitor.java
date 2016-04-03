package hyflow.caesar.statistics;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;


public final class QueueMonitor implements Runnable {
    private static final Logger logger =
            LogManager.getLogger(QueueMonitor.class);
    private static QueueMonitor instance = new QueueMonitor();
    private final Map<String, Collection> queues = new TreeMap<>();
    private final Map<String, Map> maps = new TreeMap<>();


    private QueueMonitor() {
        new Thread(this).start();
    }

    public static QueueMonitor getInstance() {
        return instance;
    }

    public void registerQueue(String name, Collection queue) {
        logger.warn("Registering: " + name + ", " + queue.getClass());
        synchronized (queues) {
            queues.put(name, queue);
        }
    }

    public void registerMap(String name, Map map) {
        logger.warn("Registering: " + name + ", " + map.getClass());
        synchronized (maps) {
            maps.put(name, map);
        }
    }

    @Override
    public void run() {
        long start = System.currentTimeMillis();
        try {
            Thread.sleep(10000);
            StringBuilder sb = new StringBuilder();
            String[] qkeys = queues.keySet().toArray(new String[]{});
            String[] mkeys = maps.keySet().toArray(new String[]{});
            String sep = "% ";
            for (String tName : qkeys) {
                sb.append(sep).append(tName);
                sep = "\t";
            }
            for (String tName : mkeys) {
                sb.append(sep).append(tName);
                sep = "\t";
            }
            sb.append("\talpha\n");
            logger.fatal(sb.toString());
            while (true) {
                Thread.sleep(1000);
                int time = (int) (System.currentTimeMillis() - start);
                sb = new StringBuilder();
                sb.append(time + "\t");
                for (String key : qkeys) {
                    sb.append(queues.get(key).size() + "\t");
                }
                for (String key : mkeys) {
                    sb.append(maps.get(key).size() + "\t");
                }
                sb.append("\n");
                logger.fatal(sb.toString());
            }
        } catch (InterruptedException e) {
        }
    }
}
