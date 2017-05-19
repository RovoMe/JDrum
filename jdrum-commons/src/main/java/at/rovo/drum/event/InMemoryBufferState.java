package at.rovo.drum.event;

import at.rovo.drum.Broker;

/**
 * The current state the {@link Broker brokers} internal buffer is currently in.
 *
 * @author Roman Vottner
 */
public enum InMemoryBufferState
{
    /**
     * Signals that the internal buffer of the {@link Broker} is currently empty
     **/
    EMPTY,
    /**
     * Signals that the internal buffer of the {@link Broker} has still room for additional entries before giving a
     * warning
     **/
    WITHIN_LIMIT,
    /**
     * Signals that the internal buffer of the {@link Broker} exceeded its limits. It will still accept further entries,
     * though administrators are recommended to investigate into why the buffer isn't emptied
     **/
    EXCEEDED_LIMIT,
    /**
     * Signals that the {@link Broker} has declared to stop its work and won't accept new data entries
     **/
    STOPPED
}
