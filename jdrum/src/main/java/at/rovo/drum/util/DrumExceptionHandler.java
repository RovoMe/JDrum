package at.rovo.drum.util;

import java.lang.Thread.UncaughtExceptionHandler;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Takes care of handling any exceptions caught in threads monitored by Javas
 * {@link java.util.concurrent.ExecutorService}
 *
 *
 * @author Roman Vottner
 */
public class DrumExceptionHandler implements UncaughtExceptionHandler
{
    /** The logger of this class **/
    private final static Logger LOG = LogManager.getLogger(DrumExceptionHandler.class);

    @Override
    public void uncaughtException(Thread t, Throwable e)
    {
        // log the error and exit the application
        if (LOG.isErrorEnabled())
        {
            LOG.error("Exception in Thread: " + t.getName() + "; Reason: " +
                      e.getClass().getName() + " - " + e.getLocalizedMessage());
            LOG.catching(Level.ERROR, e);
        }
    }
}
