package at.rovo.caching.drum;

import java.lang.Thread.UncaughtExceptionHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DrumExceptionHandler implements UncaughtExceptionHandler
{
	private final static Logger logger = LogManager.getLogger(
			DrumExceptionHandler.class);
	
	@Override
	public void uncaughtException(Thread t, Throwable e)
	{
		logger.error("Exception in Thread: "+t.getName()+"; Reason: "+
				e.getClass().getName()+" - "+e.getLocalizedMessage(),e);
		e.printStackTrace();
		System.exit(1);
	}
}
