package at.rovo.caching.drum.event;

import at.rovo.caching.drum.Broker;
import at.rovo.caching.drum.DiskWriter;

/**
 * The current state a {@link DiskWriter} is in.
 *
 * @author Roman Vottner
 */
public enum DiskWriterState
{
    /**
     * Signals that the disk writer has not written or reset the total written bytes
     **/
    EMPTY,
    /**
     * Signals that the disk writer is currently waiting on data from the {@link Broker}
     **/
    WAITING_ON_DATA,
    /**
     * Signals that the disk writer received data from the {@link Broker} and is starting to process them
     **/
    DATA_RECEIVED,
    /**
     * Signals that the disk writer is currently waiting for the lock of the disk bucket file from the {@link
     * at.rovo.caching.drum.Merger}, which is currently merging all disk bucket files into its central data store
     **/
    WAITING_ON_LOCK,
    /**
     * Signals that the disk writer is currently writing previously fetched data from the {@link Broker} into the
     * own disk bucket file
     **/
    WRITING,
    /**
     * Signals that the disk writer has finished its work and wont take further data to write into the local disk
     * bucket file
     **/
    FINISHED,
    /**
     * Signals that the disk writer has stopped due to an unexpected error
     **/
    FINISHED_WITH_ERROR
}
