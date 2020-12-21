package at.rovo.drum;

import at.rovo.drum.util.DiskFileHandle;

/**
 * Defines method needed to store and retrieve {@link DrumStoreEntry entries} to or from a disk bucket file which caches
 * the data object until it is merged into a backing data storage using an {@link Merger} implementation.
 * <p>
 * Note that the disk bucket files should be merged with the backing data store once a disk bucket file exceeds a
 * certain threshold. The merging of the disk bucket files occur sequentially from 0 to n, where n is the number of
 * defined disk bucket files.
 *
 * @author Roman Vottner
 */
public interface DiskWriter extends Runnable, Stoppable {

    /**
     * Returns the ID of the bucket the writer is responsible for.
     *
     * @return The bucket ID of the writer
     */
    int getBucketId();

    /**
     * Returns a handle to the disk bucket files managed by this writer.
     *
     * @return A handle to the disk bucket files
     */
    DiskFileHandle getDiskFiles();

    /**
     * Returns the number of bytes written to the key/value disk file.
     *
     * @return The number of bytes written to the key/value disk file
     */
    long getKVFileBytesWritten();

    /**
     * Returns the number of bytes written to the auxiliary data disk file.
     *
     * @return The number of bytes written to the auxiliary data disk file
     */
    long getAuxFileBytesWritten();

    /**
     * Resets the cursor of the key/value and auxiliary data disk files to the start of the file.
     * <p>
     * This method should be called either to clear the content or after the data has been persisted.
     */
    void reset();

    /**
     * Closes resources held by the instance.
     */
    void close();
}
