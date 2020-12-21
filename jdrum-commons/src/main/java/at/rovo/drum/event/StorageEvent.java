package at.rovo.drum.event;

import at.rovo.drum.DrumListener;

import javax.annotation.Nonnull;

/**
 * Informs {@link DrumListener listeners} about the new total size of unique entries within the data store.
 *
 * @author Roman Vottner
 */
public class StorageEvent extends DrumEvent<StorageEvent> {

    /**
     * The number of entries stored in the data store
     */
    private final long numEntries;

    /**
     * Initializes a new storage event that notifies the listeners about the new total number of unique entries within
     * the backing data store.
     *
     * @param drumName   The name of the DRUM instance this event was issued from
     * @param numEntries The new total number of unique entries within the data store
     */
    public StorageEvent(@Nonnull final String drumName, long numEntries) {
        super(drumName, StorageEvent.class);
        this.numEntries = numEntries;
    }

    /**
     * Returns the number of unique entries in the data store.
     *
     * @return The number of entries in the data store
     */
    public long getNumberOfEntries() {
        return this.numEntries;
    }
}
