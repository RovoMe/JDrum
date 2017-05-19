package at.rovo.drum.event;

import at.rovo.drum.DiskWriter;
import at.rovo.drum.DrumListener;

/**
 * Informs {@link DrumListener listeners} on a state change of a {@link DiskWriter}.
 *
 * @author Roman Vottner
 */
public class DiskWriterStateUpdate extends DrumEvent<DiskWriterStateUpdate>
{
    /** The bucket ID the event was triggered from **/
    private int bucketId = 0;
    /** The new state of the disk writer **/
    private DiskWriterState state = null;

    /**
     * Initializes a event for a status change of the disk writer.
     *
     * @param drumName
     *         The name of the DRUM instance this event was issued from
     * @param bucketId
     *         The identifier of the bucket the event was triggered from
     * @param state
     *         The new state of the disk writer
     */
    public DiskWriterStateUpdate(String drumName, int bucketId, DiskWriterState state)
    {
        super(drumName, DiskWriterStateUpdate.class);
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
     * The new state the disk writer is in.
     *
     * @return The new state of the disk writer
     */
    public DiskWriterState getState()
    {
        return this.state;
    }

    @Override
    public String toString()
    {
        return this.drumName + " - " + this.currentThread.getName() + " - DiskWriter " + this.bucketId +
               " state changed to: " + this.state;
    }
}
