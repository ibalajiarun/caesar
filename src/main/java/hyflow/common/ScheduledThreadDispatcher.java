package hyflow.common;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Adds debugging functionality to the standard
 * {@link ScheduledThreadPoolExecutor}. The additional debugging support
 * consists of naming the thread used by the executor and checking if the
 * current thread executing is the executor thread. It also limits the number of
 * threads on the pool to one.
 *
 * @author Nuno Santos (LSR)
 */
public class ScheduledThreadDispatcher extends ScheduledThreadPoolExecutor {
    private final static Logger logger = LogManager.getLogger(ScheduledThreadDispatcher.class);

    private final NamedThreadFactory ntf;

    public ScheduledThreadDispatcher(String threadName, int count) {
        super(count, new NamedThreadFactory(threadName));
        logger.fatal("Thread count " + count);
        ntf = (NamedThreadFactory) getThreadFactory();
        setRejectedExecutionHandler((Runnable r, ThreadPoolExecutor executor) -> {
                logger.fatal("Task rejected: " + r);
        });
    }

    private final static class NamedThreadFactory implements ThreadFactory {
        final String name;
        private Thread lastCreatedThread;
        private int id;

        public NamedThreadFactory(String name) {
            this.name = name;
            this.id = 0;
        }

        public Thread newThread(Runnable r) {
            // Name the thread and save a reference to it for debugging
            lastCreatedThread = new Thread(r, name + id);
            id++;
            return lastCreatedThread;
        }
    }

}
