package at.rovo.caching.drum;

public class DiskWriterDispatchInvoker<T extends InMemoryData<V,A>,V extends ByteSerializer<V>,A extends ByteSerializer<A>> implements Runnable
{
	private InMemoryMessageBroker<T,V,A> broker = null;
	private volatile boolean poll = false;
	private volatile boolean stopRequested = false;
	
	public DiskWriterDispatchInvoker(InMemoryMessageBroker<T,V,A> broker)
	{
		this.broker = broker;
	}
	
	@Override
	public void run()
	{
		while(!stopRequested)
		{
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
			else
			{
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
	
	public synchronized void startPolling()
	{
		boolean alreadyPolling = this.poll;
		this.poll = true;
		if (!alreadyPolling)
			this.notify();
	}
	
	public void stopPolling()
	{
		this.poll = false;
	}
	
	public boolean isPolling()
	{
		return this.poll;
	}
	
	public void stop()
	{
		this.stopRequested = true;
	}
}
