package at.rovo.caching.drum.event;

import at.rovo.caching.drum.DiskWriter;
import at.rovo.caching.drum.Merger;

/**
 * The current state the {@link Merger} is in.
 *
 * @author Roman Vottner
 */
public enum MergerState
{
    /**
     * Signals that the merger is waiting on a {@link DiskWriter} to signal a merge due to exceeding its disk bucket
     * file size threshold
     **/
    WAITING_ON_MERGE_REQUEST,
    /**
     * Signals that a {@link DiskWriter} requested a merge due to exceeding the file size limit
     **/
    MERGE_REQUESTED,
    /**
     * Signals that the {@link Merger} is waiting on a {@link DiskWriter} to finish writing to its disk bucket file and
     * releasing the lock
     **/
    WAITING_ON_LOCK,
    /**
     * Signals that the {@link Merger} acquired the lock of the disk bucket file and is merging the data from the disk
     * bucket file into its backing data store
     **/
    MERGING,
    /**
     * Signals that the {@link Merger} has stopped due to an unexpected error
     **/
    FINISHED_WITH_ERRORS,
    /**
     * Signals that the {@link Merger} has finished its work and wont merge disk bucket files with its backing data
     * store any further
     **/
    FINISHED
}
