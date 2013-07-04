package at.rovo.caching.drum.internal;

import at.rovo.caching.drum.data.ByteSerializer;

/**
 * <p>
 * <em>DiskWriterDispatchInvoker</em> takes a reference to an
 * {@link InMemoryMessageBroker} instance which it polls every 10 milliseconds
 * if the instance was requested to poll the broker by invoking the
 * {@link #startPolling()} method. Polling can be paused using 
 * {@link #stopPolling()}.
 * </p>
 * <p>Note that this instance will not terminate until it is requested through
 * invoking {@link #stop()}.</p>
 * 
 * @author Roman Vottner
 * 
 * @param <T>
 *            The type of the data the broker manages
 * @param <V>
 *            The type of the value
 * @param <A>
 *            The type of the auxiliary data attached to a key
 */
public class DiskWriterDispatchInvoker<T extends InMemoryData<V, A>, V extends 
	ByteSerializer<V>, A extends ByteSerializer<A>> implements Runnable
{
	/** Reference to the IBroker instance to poll **/
	private InMemoryMessageBroker<T, V, A> broker = null;
	/**
	 * If set to true, starts the polling of the IBroker invokeDispatch method
	 **/
	private volatile boolean poll = false;
	/**
	 * If set to true, indicates that the work of the started thread should
	 * finish
	 **/
	private volatile boolean stopRequested = false;

	/**
	 * <p>
	 * Initializes a new instance of this helper class.
	 * </p>
	 * 
	 * @param broker
	 *            The broker to poll every 10 milliseconds for available data to
	 *            dispatch if the poll field is set to true
	 */
	public DiskWriterDispatchInvoker(InMemoryMessageBroker<T, V, A> broker)
	{
		this.broker = broker;
	}

	@Override
	public void run()
	{
		// as long as no stop has been requested
		while (!stopRequested)
		{
			// check if we should poll the broker for data to dispatch
			if (this.poll)
			{
				this.broker.invokeDispatch();
				try
				{
					Thread.sleep(10);
				}
				catch (InterruptedException e)
				{
					e.printStackTrace();
				}
			}
			// none or not enough data available, so wait until a poll request
			// is set
			else
			{
				// wait and notify need to be within a synchronized block (or at
				// least need to be guaranteed to have exclusive thread control)
				synchronized (this)
				{
					try
					{
						this.wait();
					}
					catch (InterruptedException e)
					{

					}
				}
			}
		}
	}

	/**
	 * <p>
	 * Signals the instance to start polling the broker instance to invoke its
	 * dispatch method.
	 * </p>
	 */
	public synchronized void startPolling()
	{
		boolean alreadyPolling = this.poll;
		this.poll = true;
		if (!alreadyPolling)
			this.notify();
	}

	/**
	 * <p>
	 * Signals the instance to pause polling the broker instance to invoke its
	 * dispatch method.
	 * </p>
	 */
	public void stopPolling()
	{
		this.poll = false;
	}

	/**
	 * <p>
	 * Returns if the instance is currently polling the broker to invoke its
	 * dispatch method.
	 * </p>
	 * 
	 * @return True if this instance is currently polling the broker, false
	 *         otherwise
	 */
	public boolean isPolling()
	{
		return this.poll;
	}

	/**
	 * <p>
	 * Requests this instance to terminate its backing thread and to terminate
	 * regularly.
	 * </p>
	 */
	public void stop()
	{
		this.stopRequested = true;
		synchronized(this)
		{
			this.notify();
		}
	}
}
