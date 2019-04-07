package at.rovo.drum;

import java.io.Serializable;

/**
 * This extension of {@link Drum} adds notification support on certain statistics to DRUM. An implementation of this
 * interface should be used in cases where insights on the internals of DRUM should are necessary.
 */
public interface StatisticsEnabledDrum<V extends Serializable, A extends Serializable> extends Drum<V, A> {

    /**
     * Adds an object to the {@link java.util.Set Set} of objects to be notified on state or statistic changes.
     *
     * @param listener A {@link DrumListener} instance to notify on internal state changes
     */
    void addDrumListener(DrumListener listener);

    /**
     * Removes a previously added object from the {@link java.util.Set Set} of objects which require notifications on
     * state or statistic changes.
     *
     * @param listener A {@link DrumListener} instance to be removed from the set of registered listeners
     */
    void removeDrumListener(DrumListener listener);
}
