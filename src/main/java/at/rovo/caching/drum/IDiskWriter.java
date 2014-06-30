package at.rovo.caching.drum;

import java.io.RandomAccessFile;
import java.util.concurrent.Semaphore;
import at.rovo.caching.drum.data.ByteSerializer;
import at.rovo.caching.drum.internal.IDrumRuntimeListener;
import at.rovo.caching.drum.internal.InMemoryData;

/**
 * <p>
 * Defines method needed to store and retrieve {@link InMemoryData} to or from a
 * disk bucket file which caches the data object until it is merged into a
 * backing data storage using an {@link IMerger} implementation.
 * </p>
 * <p>
 * Note that the disk bucket files should be merged with the backing data store
 * once a disk bucket file exceeds a certain threshold. The merging of the disk
 * bucket files occur sequentially from 0 to n, where n is the number of defined
 * disk bucket files.
 * </p>
 * 
 * @author Roman Vottner
 * 
 * @param <V>
 *            The type of the value object
 * @param <A>
 *            The type of the auxiliary data object
 */
public interface IDiskWriter<V extends ByteSerializer<V>, A extends ByteSerializer<A>>
		extends Runnable, IDrumRuntimeListener
{
	/**
	 * <p>
	 * Returns the ID of the bucket the writer is responsible for.
	 * </p>
	 * 
	 * @return The bucket ID of the writer
	 */
	public int getBucketId();

	/**
	 * <p>
	 * Shares the locking mechanism with a caller to access the managed disk
	 * file.
	 * </p>
	 * 
	 * @return The lock used to get access to the managed disk file.
	 */
	public Semaphore accessDiskFile();

	/**
	 * <p>
	 * Returns the name of the managed key/value disk file.
	 * </p>
	 * 
	 * @return The name of the managed key/value disk file
	 */
	public String getKVFileName();

	/**
	 * <p>
	 * Returns the name of the managed auxiliary data disk file.
	 * </p>
	 * 
	 * @return The name of the managed auxiliary data disk file
	 */
	public String getAuxFileName();

	/**
	 * <p>
	 * Returns a reference to the bucket file which stores key/value data.
	 * </p>
	 * 
	 * @return The reference to the key/value bucket file
	 */
	public RandomAccessFile getKVFile();

	/**
	 * <p>
	 * Returns a reference to the bucket file which stores the auxiliary data
	 * that is attached to a key
	 * </p>
	 * 
	 * @return The reference to the auxiliary bucket file
	 */
	public RandomAccessFile getAuxFile();

	/**
	 * <p>
	 * Returns the number of bytes written to the key/value disk file.
	 * </p>
	 * 
	 * @return The number of bytes written to the key/value disk file
	 */
	public long getKVFileBytesWritten();

	/**
	 * <p>
	 * Returns the number of bytes written to the auxiliary data disk file.
	 * </p>
	 * 
	 * @return The number of bytes written to the auxiliary data disk file
	 */
	public long getAuxFileBytesWritte();

	/**
	 * <p>
	 * Resets the cursor of the key/value and auxiliary data disk files to the
	 * start of the file.
	 * </p>
	 * <p>
	 * This method should be called either to clear the content or after the
	 * data has been persisted.
	 * </p>
	 */
	public void reset();

	/**
	 * <p>
	 * Closes resources held by the instance.
	 * </p>
	 */
	public void close();
}
