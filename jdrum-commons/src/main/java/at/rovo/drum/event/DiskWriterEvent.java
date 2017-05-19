package at.rovo.drum.event;

import at.rovo.drum.DrumListener;

/**
 * Informs {@link DrumListener listeners} that the disk bucket writer has written new bytes to the
 * temporary bucket file. {@link #getBucketId()} will return the bucket ID of the file the data were written to while
 * {@link #getKVBytes()} and {@link #getAuxBytes()} will return the number of bytes written for key/value and auxiliary
 * data parts.
 *
 * @author Roman Vottner
 */
public class DiskWriterEvent extends DrumEvent<DiskWriterEvent>
{
    /** The bucket ID the event was triggered from **/
    private int bucketId = 0;
    /** The length of the key/value bytes of the bucket when the event occurred **/
    private long kvBytes = 0L;
    /** The length of the auxiliary data in bytes of the respective bucket when the event occurred **/
    private long auxBytes = 0L;

    /**
     * Initializes a new disk writer event for the given DRUM instance' bucket ID.
     *
     * @param drumName
     *         The name of the DRUM instance this event was issued from
     * @param bucketId
     *         The identifier of the bucket the event was triggered from
     * @param kvBytes
     *         The number of bytes of the key/value entry when the event occurred
     * @param auxBytes
     *         The number of bytes of the auxiliary data when the event occurred
     */
    public DiskWriterEvent(String drumName, int bucketId, long kvBytes, long auxBytes)
    {
        super(drumName, DiskWriterEvent.class);
        this.bucketId = bucketId;
        this.kvBytes = kvBytes;
        this.auxBytes = auxBytes;
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
     * The byte length of the key/value pair processed when the event occurred.
     *
     * @return The length of the key/value pair in bytes
     */
    public long getKVBytes()
    {
        return this.kvBytes;
    }

    /**
     * The byte length of the auxiliary data processed when the event occurred.
     *
     * @return The length of the auxiliary data in bytes
     */
    public long getAuxBytes()
    {
        return this.auxBytes;
    }

    @Override
    public String toString()
    {
        return this.drumName + " - " + this.currentThread.getName() + " - DiskWriter " + this.bucketId + " has written "
               + this.kvBytes + " bytes into bucket" + this.bucketId + ".kv file and " + this.auxBytes +
               " bytes into bucket" + this.bucketId + ".aux file";
    }
}
