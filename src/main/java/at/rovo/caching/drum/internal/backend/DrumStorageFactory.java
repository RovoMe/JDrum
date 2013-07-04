package at.rovo.caching.drum.internal.backend;

import at.rovo.caching.drum.DrumException;
import at.rovo.caching.drum.IDispatcher;
import at.rovo.caching.drum.IMerger;
import at.rovo.caching.drum.data.ByteSerializer;
import at.rovo.caching.drum.event.DrumEventDispatcher;
import at.rovo.caching.drum.internal.backend.berkeley.BerkeleyDBStorageFactory;
import at.rovo.caching.drum.internal.backend.cacheFile.CacheFileStorageFactory;

/**
 * <p>
 * A basic Factory class for creating a DRUM backing data storage.
 * </p>
 * <p>
 * Currently only a Berkeley DB and a self written CacheFile storage are
 * available. By default the CacheFile storage can be created through invoking
 * {@link #getDefaultStorageFactory(String, int, IDispatcher, Class, Class, DrumEventDispatcher)}
 * </p>
 * 
 * @author Roman Vottner
 * 
 * @param <V>
 *            The type of the value
 * @param <A>
 *            The type of the auxiliary data attached to a key
 * @see BerkeleyDBStorageFactory
 * @see CacheFileStorageFactory
 */
public abstract class DrumStorageFactory<V extends ByteSerializer<V>, A extends
	ByteSerializer<A>>
{
	/** The created merger implementation **/
	protected IMerger<V, A> merger = null;

	/**
	 * <p>
	 * On creating a new instance of this class, it will trigger
	 * {@link #create(String, int, IDispatcher, Class, Class, DrumEventDispatcher)}
	 * and therefore delegate the creation of the concrete implementation to its
	 * child class.
	 * </p>
	 * 
	 * @param drumName
	 *            The name of the DRUM instance
	 * @param numBuckets
	 *            The number of bucket files used to store the data
	 * @param dispatcher
	 *            A reference to the {@link IDispatcher} instance that will
	 *            dispatch the results
	 * @param valueClass
	 *            The class of the value type
	 * @param auxClass
	 *            The class of the auxiliary data type
	 * @param eventDispatcher
	 *            A reference to the {@link DrumEventDispatcher} that will
	 *            forward certain DRUM events like merge status changed or disk
	 *            writer events
	 * @throws DrumException
	 *             If the backing data store could not be created
	 */
	public DrumStorageFactory(String drumName, int numBuckets,
			IDispatcher<V, A> dispatcher, Class<V> valueClass,
			Class<A> auxClass, DrumEventDispatcher eventDispatcher)
			throws DrumException
	{
		this.create(drumName, numBuckets, dispatcher, valueClass, auxClass,
				eventDispatcher);
	}

	/**
	 * <p>
	 * Forces a subclass to implement the creation of a new instance of a
	 * backing data storage.
	 * </p>
	 * 
	 * @param drumName
	 *            The name of the DRUM instance
	 * @param numBuckets
	 *            The number of bucket files used to store the data
	 * @param dispatcher
	 *            A reference to the {@link IDispatcher} instance that will
	 *            dispatch the results
	 * @param valueClass
	 *            The class of the value type
	 * @param auxClass
	 *            The class of the auxiliary data type
	 * @param eventDispatcher
	 *            A reference to the {@link DrumEventDispatcher} that will
	 *            forward certain DRUM events like merge status changed or disk
	 *            writer events
	 * @throws DrumException
	 *             If the backing data store could not be created
	 */
	protected abstract void create(String drumName, int numBuckets,
			IDispatcher<V, A> dispatcher, Class<V> valueClass,
			Class<A> auxClass, DrumEventDispatcher eventDispatcher)
			throws DrumException;

	/**
	 * <p>
	 * Returns the generated data storage.
	 * </p>
	 * 
	 * @return The generated data storage
	 */
	public IMerger<V, A> getStorage()
	{
		return this.merger;
	}

	/**
	 * <p>
	 * Create the default backing data store.
	 * </p>
	 * 
	 * @param drumName
	 *            The name of the DRUM instance
	 * @param numBuckets
	 *            The number of bucket files used to store the data
	 * @param dispatcher
	 *            A reference to the {@link IDispatcher} instance that will
	 *            dispatch the results
	 * @param valueClass
	 *            The class of the value type
	 * @param auxClass
	 *            The class of the auxiliary data type
	 * @param eventDispatcher
	 *            A reference to the {@link DrumEventDispatcher} that will
	 *            forward certain DRUM events like merge status changed or disk
	 *            writer events
	 * @return A reference to the default backing data storage, which is the
	 *         CacheFile data store
	 * @throws DrumException
	 *             If the backing data store could not be created
	 */
	public static <V extends ByteSerializer<V>, A extends 
		ByteSerializer<A>> DrumStorageFactory<V, A> getDefaultStorageFactory(
			String drumName, int numBuckets, IDispatcher<V, A> dispatcher,
			Class<V> valueClass, Class<A> auxClass,
			DrumEventDispatcher eventDispatcher) throws DrumException
	{
		return new CacheFileStorageFactory<>(drumName, numBuckets, dispatcher,
				valueClass, auxClass, eventDispatcher);

	}
}
