package at.rovo.drum.event;

import at.rovo.drum.DrumListener;
import javax.annotation.Nonnull;

/**
 * A <em>DrumEvent</em> is an event triggered during processing of the passed data. The event is intended for {@link
 * DrumListener} which want to be notified on internal state changes to monitor the current state of the system.
 *
 * @param <T> The concrete type of the event
 * @author Roman Vottner
 */
public abstract class DrumEvent<T extends DrumEvent<T>> {

    /**
     * The name of the DRUM instance the event was triggered from
     */
    protected String drumName;
    /**
     * The class of the respective event type
     */
    private final Class<T> clazz;
    /**
     * The thread the event was triggered from
     */
    protected final Thread currentThread = Thread.currentThread();

    /**
     * Creates a new DRUM event object.
     *
     * @param drumName The name of the DRUM instance the event was triggered from
     * @param clazz    The class of the event type
     */
    DrumEvent(@Nonnull final String drumName, @Nonnull final Class<T> clazz) {
        this.drumName = drumName;
        this.clazz = clazz;
    }

    /**
     * The name of the DRUM instance that triggered the event.
     *
     * @return The name of the DRUM instance
     */
    @Nonnull
    public String getDrumName() {
        return this.drumName;
    }

    /**
     * The class of the event type.
     *
     * @return The class of the evnet type
     */
    @Nonnull
    public Class<T> getRealClass() {
        return this.clazz;
    }

    /**
     * A reference to the thread the event was triggered from.
     *
     * @return The thread that triggered the event
     */
    @Nonnull
    public Thread getThread() {
        return this.currentThread;
    }

    @Override
    @Nonnull
    public String toString() {
        return "DrumEvent[name=" + this.drumName + ", clazz=" + clazz + ", thread=" + currentThread.getName() + "]";
    }
}
