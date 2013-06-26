package at.rovo.caching.drum.event;

import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.LinkedBlockingQueue;
import at.rovo.caching.drum.IDrumListener;

public class DrumEventDispatcher implements Runnable, DrumEventListener
{
	private Set<IDrumListener> listeners = new CopyOnWriteArraySet<>();
	
	private BlockingQueue<DrumEvent<? extends DrumEvent<?>>> events = new LinkedBlockingQueue<>();
	private volatile boolean stopRequested = false;
	
	public DrumEventDispatcher()
	{
		
	}
	
	public void addDrumListener(IDrumListener listener)
	{
		this.listeners.add(listener);
	}
	
	public void removeDrumListener(IDrumListener listener)
	{
		this.listeners.remove(listener);
	}
	
	@Override
	public void update(DrumEvent<? extends DrumEvent<?>> event)
	{
		this.events.add(event);
	}

	@Override
	public void run()
	{
		while (!this.stopRequested)
		{
			DrumEvent<?> event;
			try
			{
				event = this.events.take();
				if (event != null)
				{					
					for (IDrumListener listener : this.listeners)
						listener.update(event);	
				}
			}
			catch (InterruptedException e)
			{

			}
		}
	}
	
	public void stop()
	{
		this.stopRequested = true;
	}
}
