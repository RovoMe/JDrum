package at.rovo.caching.drum.util;

import java.lang.Thread.UncaughtExceptionHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import at.rovo.caching.drum.internal.DiskBucketWriter;
import at.rovo.caching.drum.internal.DiskFileMerger;

/**
 * <p>Catches any uncaught exceptions during the execution of the threaded 
 * instances of {@link DiskFileMerger} or {@link DiskBucketWriter}.</p>
 * 
 * @author Roman Vottner
 */
public class DrumExceptionHandler implements UncaughtExceptionHandler
{
	/** The logger of this class **/
	private final static Logger logger = LogManager.getLogger(
			DrumExceptionHandler.class);
	
	@Override
	public void uncaughtException(Thread t, Throwable e)
	{
		// log the error and exit the application
		if (logger.isErrorEnabled())
			logger.error("Exception in Thread: "+t.getName()+"; Reason: "+
				e.getClass().getName()+" - "+e.getLocalizedMessage(),e);
		e.printStackTrace();
		System.exit(1);
	}
}
