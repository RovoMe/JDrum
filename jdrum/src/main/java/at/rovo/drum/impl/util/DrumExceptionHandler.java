package at.rovo.drum.impl.util;

import java.lang.Thread.UncaughtExceptionHandler;
import java.lang.invoke.MethodHandles;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Takes care of handling any exceptions caught in threads monitored by Javas
 * {@link java.util.concurrent.ExecutorService}
 *
 * @author Roman Vottner
 */
public class DrumExceptionHandler implements UncaughtExceptionHandler {
    /**
     * The logger of this class
     **/
    private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    @Override
    public void uncaughtException(Thread t, Throwable e) {
        // log the error and exit the application
        if (LOG.isErrorEnabled()) {
            LOG.error("Exception in Thread: " + t.getName() + "; Reason: " +
                    e.getClass().getName() + " - " + e.getLocalizedMessage(), e);
        }
    }
}
