package at.rovo.caching.drum;

import at.rovo.caching.drum.event.DrumEvent;
import java.io.IOException;

/**
 * The null event dispatcher implements the null object design pattern that offers suitable default do nothing behavior
 * on internal event updates. Its main purpose is to provide a base class to extend from to limit the number of methods
 * to override from.
 *
 * @author Roman Vottner
 */
public class NullEventDispatcher implements DrumEventDispatcher
{
    @Override
    public void addDrumListener(DrumListener listener)
    {
        // do nothing
    }

    @Override
    public void removeDrumListener(DrumListener listener)
    {
        // do nothing
    }

    @Override
    public void update(DrumEvent<? extends DrumEvent<?>> event)
    {
        // do nothing
    }

    @Override
    public void run()
    {
        // do nothing
    }

    @Override
    public void close() throws IOException
    {
        // do nothing
    }
}
