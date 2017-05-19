package at.rovo.drum.event;

import at.rovo.drum.Broker;

/**
 * An event triggered by the {@link Broker} implementation when the internal data buffer either exceeds its limit or got
 * emptied.
 *
 * @author Roman Vottner
 */
public class InMemoryBufferStateUpdate extends DrumEvent<InMemoryBufferStateUpdate>
{
    /** The bucket ID the event was triggered from **/
    private int bucketId = 0;
    /** The new state the broker is in **/
    private InMemoryBufferState state = null;

    /**
     * Initializes a new event triggered by the {@link at.rovo.caching.drum.Broker} when the state of the internal
     * buffer changed.
     *
     * @param drumName
     *         The name of the DRUM instance this event was issued from
     * @param bucketId
     *         The identifier of the bucket the event was triggered from
     * @param state
     *         The new state the {@link at.rovo.caching.drum.Broker} is in
     */
    public InMemoryBufferStateUpdate(String drumName, int bucketId, InMemoryBufferState state)
    {
        super(drumName, InMemoryBufferStateUpdate.class);
        this.bucketId = bucketId;
        this.state = state;
    }

    /**
     * The identifier of the bucket the event was issued from.
     *
     * @return The bucket identifier the event was sent from
     */
    public int getBucketId()
    {
        return this.bucketId;
    }

    /**
     * The new state the {@link Broker brokers} buffer is in.
     *
     * @return The new state of the {@link Broker brokers} buffer
     */
    public InMemoryBufferState getState()
    {
        return this.state;
    }

    @Override
    public String toString()
    {
        return this.drumName + " - " + this.currentThread.getName() + " - InMemoryBuffer " + this.bucketId
               + " state changed to: " + this.state;
    }
}
