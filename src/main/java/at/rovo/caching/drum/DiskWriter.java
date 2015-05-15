package at.rovo.caching.drum;

import at.rovo.caching.drum.data.ByteSerializer;
import at.rovo.caching.drum.internal.DrumRuntimeListener;
import at.rovo.caching.drum.internal.InMemoryData;

import java.io.RandomAccessFile;
import java.util.concurrent.Semaphore;

/**
 * Defines method needed to store and retrieve {@link InMemoryData} to or from a disk bucket file which caches the data
 * object until it is merged into a backing data storage using an {@link Merger} implementation.
 * <p>
 * Note that the disk bucket files should be merged with the backing data store once a disk bucket file exceeds a
 * certain threshold. The merging of the disk bucket files occur sequentially from 0 to n, where n is the number of
 * defined disk bucket files.
 *
 * @param <V>
 * 		The type of the value object
 * @param <A>
 * 		The type of the auxiliary data object
 *
 * @author Roman Vottner
 */
public interface DiskWriter<V extends ByteSerializer<V>, A extends ByteSerializer<A>>
		extends Runnable, DrumRuntimeListener
{
	/**
	 * Returns the ID of the bucket the writer is responsible for.
	 *
	 * @return The bucket ID of the writer
	 */
	int getBucketId();

	/**
	 * Shares the locking mechanism with a caller to access the managed disk file.
	 *
	 * @return The lock used to get access to the managed disk file.
	 */
	Semaphore accessDiskFile();

	/**
	 * Returns the name of the managed key/value disk file.
	 *
	 * @return The name of the managed key/value disk file
	 */
	String getKVFileName();

	/**
	 * Returns the name of the managed auxiliary data disk file.
	 *
	 * @return The name of the managed auxiliary data disk file
	 */
	String getAuxFileName();

	/**
	 * Returns a reference to the bucket file which stores key/value data.
	 *
	 * @return The reference to the key/value bucket file
	 */
	RandomAccessFile getKVFile();

	/**
	 * Returns a reference to the bucket file which stores the auxiliary data that is attached to a key
	 *
	 * @return The reference to the auxiliary bucket file
	 */
	RandomAccessFile getAuxFile();

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
	long getAuxFileBytesWritte();

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
