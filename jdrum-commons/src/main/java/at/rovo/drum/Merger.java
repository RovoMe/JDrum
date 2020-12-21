package at.rovo.drum;

import javax.annotation.Nonnull;

/**
 * <code>Merger</code> iterates through all registered {@link DiskWriter} instances and uses their bucket disk files to
 * extract data to merge. It therefore only merges data that was created since the last merge. New disk writer objects
 * can be added on invoking {@link #addDiskFileWriter(DiskWriter)}
 * <p>
 * The merge, by default, is done in the context of the {@link Thread} the merger instance was placed in. The execution
 * thread blocks until it is signaled about availability of data, which is done via {@link #requestMerge()}.
 * <p>
 * As <code>Merger</code> administers a data store file in the back, this file needs to be closed after usage to
 * prevent data-loss.
 *
 * @author Roman Vottner
 */
public interface Merger extends Runnable, Stoppable {

    /**
     * Adds a disk writer object to the merger instance, which is used to share a lock on the disk file both objects try
     * to access. This is necessary as the disk writer could write additional data to the file while the merger is
     * reading from the same file, which could result in an inconsistent state.
     *
     * @param writer The responsible class for writing the actual data to disk
     */
    void addDiskFileWriter(@Nonnull final DiskWriter writer);

    /**
     * Signals the merging implementation to start the merging process.
     * <p>
     * Note that execution of the merge happens in the context of the {@link Thread} the merger was placed in.
     */
    void requestMerge();

    /**
     * The merger implementation should wait for all threads it is collecting data from until they signaled their
     * completion before starting a final merge phase and stop.
     */
    void writerDone();

    /**
     * Returns the number of unique entries stored into the data store.
     *
     * @return The number of unique entries synchronized and stored within the backing data store
     */
    long getNumberUniqueEntriesStored();

    /**
     * Closes resources held by the instance.
     */
    void close();
}
