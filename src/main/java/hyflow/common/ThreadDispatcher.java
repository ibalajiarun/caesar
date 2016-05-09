package hyflow.common;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Comparator;
import java.util.concurrent.*;

/**
 * Adds debugging functionality to the standard
 * {@link ScheduledThreadPoolExecutor}. The additional debugging support
 * consists of naming the thread used by the executor and checking if the
 * current thread executing is the executor thread. It also limits the number of
 * threads on the pool to one.
 *
 * @author Nuno Santos (LSR)
 */
public class ThreadDispatcher extends ThreadPoolExecutor {
    private final static Logger logger = LogManager.getLogger(ThreadDispatcher.class.getCanonicalName());

    private final NamedThreadFactory ntf;


    public ThreadDispatcher(String threadName, int count) {
        super(count, count * 2, 1, TimeUnit.SECONDS,
                new PriorityBlockingQueue(1000000, new PriorityFutureTaskComparator()),
                new NamedThreadFactory(threadName));
        ntf = (NamedThreadFactory) getThreadFactory();
        setRejectedExecutionHandler((Runnable r, ThreadPoolExecutor executor) -> {
                logger.fatal("Task rejected: " + r);
        });
    }

    public void execute(Runnable command, int priority) {
        super.execute(new PriorityFutureTask(command, null, priority));
    }

    public Future<?> submit(Runnable task, int priority) {
        return super.submit(new PriorityFutureTask(task, null, priority));
    }

    private static class PriorityFutureTaskComparator<T extends PriorityFutureTask> implements Comparator<T> {
        @Override
        public int compare(T t1, T t2) {
            return t2.getPriority() - t1.getPriority();
        }
    }

    /**
     * Thread factory that names the thread and keeps a reference to the last
     * thread created. Intended for debugging.
     *
     * @author Nuno Santos (LSR)
     */
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

    class PriorityFutureTask<T> extends FutureTask<T> {

        volatile int priority = 0;

        public PriorityFutureTask(Runnable runnable, T result, int priority) {
            super(runnable, result);
            this.priority = priority;
        }

        public PriorityFutureTask(Callable<T> callable, int priority) {
            super(callable);
            this.priority = priority;
        }

        public int getPriority() {
            return priority;
        }
    }

}
