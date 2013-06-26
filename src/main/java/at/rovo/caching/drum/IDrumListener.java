package at.rovo.caching.drum;

import at.rovo.caching.drum.event.DrumEvent;

public interface IDrumListener
{
	public void update(DrumEvent<? extends DrumEvent<?>> event);
}
