package at.rovo.drum.util;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.concurrent.ThreadFactory;

/**
 * Provides the possibility to create a named thread pool where each added thread's name will start with the same
 * defined name and ends with the number of the thread in the form <em>ThreadPoolName-ThreadNumber</em>.
 *
 * @author Roman Vottner
 */
public class NamedThreadFactory implements ThreadFactory {

    /**
     * The name of the thread to store in a thread pool
     */
    private String name = "pool-" + Thread.currentThread().getThreadGroup().getName() + "-thread-" + this.number++;
    /**
     * The number of threads added to the pool
     */
    private int number = 0;
    /**
     * The handler to execute if any uncaught exceptions occur
     */
    private UncaughtExceptionHandler exceptionHandler = null;

    /**
     * Sets the new name of the thread.
     *
     * @param name The new name of this thread.
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Sets the handler to invoke if any uncaught exceptions occur.
     *
     * @param exceptionHandler The handler taking care of any uncaught exceptions thrown by threads in the thread pool
     */
    public void setUncaughtExceptionHandler(UncaughtExceptionHandler exceptionHandler) {
        this.exceptionHandler = exceptionHandler;
    }

    @Override
    public Thread newThread(Runnable r) {
        Thread thread = new Thread(r, this.name + (this.name.contains("Writer") ? "-" + this.number++ : ""));
        if (this.exceptionHandler != null) {
            thread.setUncaughtExceptionHandler(this.exceptionHandler);
        }
        return thread;
    }
}
