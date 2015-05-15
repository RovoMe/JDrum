package at.rovo.caching.drum.internal.backend.berkeley;

import at.rovo.caching.drum.Dispatcher;
import at.rovo.caching.drum.DrumException;
import at.rovo.caching.drum.data.ByteSerializer;
import at.rovo.caching.drum.event.DrumEventDispatcher;
import at.rovo.caching.drum.internal.backend.DrumStorageFactory;

/**
 * <em>BerkeleyDBStorageFactory</em> is an implementation of {@link DrumStorageFactory} and takes care of initializing a
 * proper instance of a Berkeley DB cache file merger. This returned {@link at.rovo.caching.drum.Merger} instance can
 * then be used to store and/or compare the data currently cached in bucket files with the data stored in a backing
 * Berkeley DB.
 *
 * @param <V>
 * 		The type of the value
 * @param <A>
 * 		The type of the auxiliary data attached to a key
 *
 * @author Roman Vottner
 */
public class BerkeleyDBStorageFactory<V extends ByteSerializer<V>, A extends ByteSerializer<A>>
		extends DrumStorageFactory<V, A>
{
	/**
	 * Creates a new instance of a Berkeley DB storage factory that will take care of creating a new Berkeley DB cache
	 * file merger instance.
	 *
	 * @param drumName
	 * 		The name of the DRUM instance
	 * @param numBuckets
	 * 		The number of bucket files used to store the data
	 * @param dispatcher
	 * 		A reference to the {@link at.rovo.caching.drum.Dispatcher} instance that will dispatch the results
	 * @param valueClass
	 * 		The class of the value type
	 * @param auxClass
	 * 		The class of the auxiliary data type
	 * @param eventDispatcher
	 * 		A reference to the {@link DrumEventDispatcher} that will forward certain DRUM events like merge status changed
	 * 		or disk writer events
	 *
	 * @throws DrumException
	 * 		If the backing data store could not be created
	 */
	public BerkeleyDBStorageFactory(String drumName, int numBuckets, Dispatcher<V, A> dispatcher,
									Class<? super V> valueClass, Class<? super A> auxClass,
									DrumEventDispatcher eventDispatcher) throws DrumException
	{
		super(drumName, numBuckets, dispatcher, valueClass, auxClass, eventDispatcher);
	}

	@Override
	protected void create(String drumName, int numBuckets, Dispatcher<V, A> dispatcher, Class<? super V> valueClass,
						  Class<? super A> auxClass, DrumEventDispatcher eventDispatcher) throws DrumException
	{
		this.merger =
				new BerkeleyCacheFileMerger<>(drumName, numBuckets, dispatcher, valueClass, auxClass, eventDispatcher);
	}
}
