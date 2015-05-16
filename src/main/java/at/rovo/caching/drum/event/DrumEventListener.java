package at.rovo.caching.drum.event;

public interface DrumEventListener
{
    void update(DrumEvent<? extends DrumEvent<?>> event);
}
