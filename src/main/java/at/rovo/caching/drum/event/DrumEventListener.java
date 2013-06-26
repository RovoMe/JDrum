package at.rovo.caching.drum.event;

public interface DrumEventListener
{
	public void update(DrumEvent<? extends DrumEvent<?>> event);
}
