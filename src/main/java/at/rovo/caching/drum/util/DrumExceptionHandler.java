package at.rovo.caching.drum.util;

import at.rovo.caching.drum.internal.DiskBucketWriter;
import at.rovo.caching.drum.internal.DiskFileMerger;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.Thread.UncaughtExceptionHandler;

/**
 * Catches any uncaught exceptions during the execution of the threaded instances of {@link DiskFileMerger} or {@link
 * DiskBucketWriter}.
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
		System.exit(1);
	}
}
