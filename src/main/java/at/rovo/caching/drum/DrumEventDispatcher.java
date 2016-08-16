package at.rovo.caching.drum;

import at.rovo.caching.drum.event.DrumEvent;
import java.io.Closeable;

/**
 * An instance of this class takes care of shipping internal state events to registered listeners.
 *
 * @author Roman Vottner
 */
public interface DrumEventDispatcher extends Runnable
{
    /**
     * Registers a new listener instance with the dispatcher. Any registered listeners want to be informed on internal
     * state changes.
     *
     * @param listener
     *         The object that declared its interest to be notified on internal state changes
     **/
    void addDrumListener(DrumListener listener);

    /**
     * Removes an object which is no longer interested in receiving notification on internal state changes from the set
     * of interested objects.
     *
     * @param listener
     *         The object to remove from the set of interested listeners
     */
    void removeDrumListener(DrumListener listener);

    /**
     * Internal objects invoke this method to inform the dispatcher about a state change. The dispatcher should now
     * inform its registered listener objects about the state change.
     *
     * @param event
     *         The internal event which should be distributed to the listener instances
     */
    void update(DrumEvent<? extends DrumEvent<?>> event);

    /**
     * Requests to stop the event dispatcher.
     */
    void stop();
}
