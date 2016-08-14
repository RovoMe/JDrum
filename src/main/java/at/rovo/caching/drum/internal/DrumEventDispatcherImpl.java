package at.rovo.caching.drum.internal;

import at.rovo.caching.drum.DrumEventDispatcher;
import at.rovo.caching.drum.DrumListener;
import at.rovo.caching.drum.event.DrumEvent;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * This {@link DrumEventDispatcher} takes care of broadcasting internal state changes received via {@link
 * #update(DrumEvent)} to all of its registered listeners. To
 * <p>
 * Any object can register its interest on internal state changes via {@link #addDrumListener(DrumListener)} and remove
 * its interest with {@link #removeDrumListener(DrumListener)}.
 *
 * @author Roman Vottner
 */
public class DrumEventDispatcherImpl implements DrumEventDispatcher
{
    /** The set of registered listener instances which need to be informed on state changes **/
    private Set<DrumListener> listeners = new CopyOnWriteArraySet<>();
    /** The queue to add internal events to and to take events from to notify the listeners **/
    private BlockingQueue<DrumEvent<? extends DrumEvent<?>>> events = new LinkedBlockingQueue<>();
    /** Indicates if the work of this instance is no longer needed and therefore should stop its work **/
    private volatile boolean stopRequested = false;

    /**
     * Creates a new instance.
     */
    public DrumEventDispatcherImpl()
    {

    }

    @Override
    public void addDrumListener(DrumListener listener)
    {
        this.listeners.add(listener);
    }

    @Override
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
            try
            {
                DrumEvent<?> event = this.events.take();
                if (event != null)
                {
                    this.listeners.forEach(listener -> listener.update(event));
                }
            }
            catch (InterruptedException e)
            {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public void close() throws IOException
    {
        this.stopRequested = true;
    }
}
