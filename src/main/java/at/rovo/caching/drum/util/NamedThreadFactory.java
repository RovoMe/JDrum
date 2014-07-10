package at.rovo.caching.drum.util;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.concurrent.ThreadFactory;

/**
 * <p>
 * Provides the possibility to create a named thread pool where each added
 * thread's name will start with the same defined name and ends with the number
 * of the thread in the form <em>ThreadPoolName-ThreadNumber</em>.
 * </p>
 * 
 * @author Roman Vottner
 */
@SuppressWarnings("unused")
public class NamedThreadFactory implements ThreadFactory
{
	/** The name of the thread to store in a thread pool **/
	private String name = "pool-"
			+ Thread.currentThread().getThreadGroup().getName() + "-thread-"
			+ this.number++;
	/** The number of threads added to the pool **/
	private int number = 0;
	/** The handler to execute if any uncaught exceptions occur **/
	private UncaughtExceptionHandler exceptionHandler = null;
	/** Flag that indicates if the priority should be increased **/
	private boolean increasePriority = false;

	/**
	 * <p>
	 * Sets the new name of the thread.
	 * </p>
	 * 
	 * @param name
	 *            The new name of this thread.
	 */
	public void setName(String name)
	{
		this.name = name;
	}

	/**
	 * <p>
	 * Sets the handler to invoke if any uncaught exceptions occur.
	 * </p>
	 * 
	 * @param exceptionHandler
	 *            The handler taking care of any uncaught exceptions thrown by
	 *            threads in the thread pool
	 */
	public void setUncaughtExceptionHanlder(
			UncaughtExceptionHandler exceptionHandler)
	{
		this.exceptionHandler = exceptionHandler;
	}

	/**
	 * <p>Increases the priority of the thread to create by 1.</p>
	 * 
	 * @param increase True defines that the priority should be increased by 1,
	 * false omits the priority increase
	 */
	public void increasePriority(boolean increase)
	{
		this.increasePriority = increase;
	}

	@Override
	public Thread newThread(Runnable r)
	{
		Thread thread = new Thread(r, this.name + "-" + this.number++);
		if (this.increasePriority)
			thread.setPriority(Math.min(10, thread.getPriority() + 1));
		if (this.exceptionHandler != null)
			thread.setUncaughtExceptionHandler(this.exceptionHandler);
		return thread;
	}
}
