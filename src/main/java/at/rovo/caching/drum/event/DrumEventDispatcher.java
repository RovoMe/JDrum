package at.rovo.caching.drum.event;

import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.LinkedBlockingQueue;

import at.rovo.caching.drum.DrumListener;

public class DrumEventDispatcher implements Runnable, DrumEventListener
{
	private Set<DrumListener> listeners = new CopyOnWriteArraySet<>();
	
	private BlockingQueue<DrumEvent<? extends DrumEvent<?>>> events = new LinkedBlockingQueue<>();
	private volatile boolean stopRequested = false;
	
	public DrumEventDispatcher()
	{
		
	}
	
	public void addDrumListener(DrumListener listener)
	{
		this.listeners.add(listener);
	}
	
	public void removeDrumListener(DrumListener listener)
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
					for (DrumListener listener : this.listeners)
					{
						listener.update(event);
					}
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
