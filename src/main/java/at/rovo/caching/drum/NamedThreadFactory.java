package at.rovo.caching.drum;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.concurrent.ThreadFactory;

public class NamedThreadFactory implements ThreadFactory
{
	private String name = "pool-"+Thread.currentThread().getThreadGroup().getName()+"-thread-"+this.number++;
	private int number = 0;
	private UncaughtExceptionHandler exceptionHandler = null;
	private boolean increasePriority = false;
	
	public void setName(String name)
	{
		this.name = name;
	}
	
	public void setUncaughtExceptionHanlder(UncaughtExceptionHandler exceptionHandler)
	{
		this.exceptionHandler = exceptionHandler;
	}
	
	public void increasePriority(boolean increase)
	{
		this.increasePriority = increase;
	}
	
	@Override
	public Thread newThread(Runnable r)
	{
		Thread thread = new Thread(r, this.name+"-"+this.number++);
		if (this.increasePriority)
			thread.setPriority(Math.min(10, thread.getPriority()+1));
		if (this.exceptionHandler != null)
			thread.setUncaughtExceptionHandler(this.exceptionHandler);
		return thread;
	}
}
