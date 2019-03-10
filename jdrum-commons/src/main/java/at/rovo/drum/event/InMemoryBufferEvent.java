package at.rovo.drum.event;

import at.rovo.drum.Broker;

/**
 * An event triggered by the {@link Broker} implementation when data was added to the broker.
 *
 * @author Roman Vottner
 */
public class InMemoryBufferEvent extends DrumEvent<InMemoryBufferEvent> {

    /**
     * The bucket ID the event was triggered from
     */
    private int bucketId;
    /**
     * The length of the key/value bytes of the buffer when the event occurred
     */
    private long kvSize;
    /**
     * The length of the auxiliary data in bytes of the respective buffer when the event occurred
     */
    private long auxSize;

    /**
     * Initializes a new in-memory buffer event for the given DRUM instance' bucket ID.
     *
     * @param drumName The name of the DRUM instance this event was issued from
     * @param bucketId The identifier of the buffer the event was triggered from
     * @param kvSize   The number of bytes of the key/value entry when the event occurred
     * @param auxSize  The number of bytes of the auxiliary data when the event occurred
     */
    public InMemoryBufferEvent(String drumName, int bucketId, long kvSize, long auxSize) {
        super(drumName, InMemoryBufferEvent.class);
        this.bucketId = bucketId;
        this.kvSize = kvSize;
        this.auxSize = auxSize;
    }

    /**
     * The identifier of the bucket the event was issued from.
     *
     * @return The bucket identifier the event was sent from
     */
    public int getBucketId() {
        return this.bucketId;
    }

    /**
     * The byte length of the key/value pair processed when the event occurred.
     *
     * @return The length of the key/value pair in bytes
     */
    public long getKVSize() {
        return this.kvSize;
    }

    /**
     * The byte length of the auxiliary data processed when the event occurred.
     *
     * @return The length of the auxiliary data in bytes
     */
    public long getAuxSize() {
        return this.auxSize;
    }

    @Override
    public String toString() {
        return this.drumName + " - " + this.currentThread.getName() + " - InMemoryBuffer " + this.bucketId +
                " new buffer size " + this.kvSize + " bytes for key/value buffer and " + this.auxSize +
                " bytes for the aux buffer";
    }
}
