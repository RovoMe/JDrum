package at.rovo.caching.drum;

import java.util.List;
import at.rovo.caching.drum.data.ByteSerializer;
import at.rovo.caching.drum.internal.IDrumRuntimeListener;
import at.rovo.caching.drum.internal.InMemoryData;

/**
 * <p>
 * The broker manages message exchange between a producer and a consumer.
 * </p>
 * <p>
 * New data is added to the broker via {@link #put(T)} and may be retrieved via
 * {@link #takeAll()}.
 * </p>
 * 
 * @param <T>
 *            The type of the data stored by the broker
 * @author Roman Vottner
 */
public interface IBroker<T extends InMemoryData<V, A>, 
		V extends ByteSerializer<V>, A extends ByteSerializer<A>>
		extends IDrumRuntimeListener
{
	/**
	 * <p>
	 * Feeds the broker with a new data item.
	 * </p>
	 * 
	 * @param data
	 *            The data item to add to the broker
	 */
	public void put(T data);

	/**
	 * <p>
	 * Returns all data stored by the broker.
	 * </p>
	 * 
	 * @return The stored data of the broker instance
	 * @throws InterruptedException
	 */
	public List<T> takeAll() throws InterruptedException;

//	/**
//	 * <p>
//	 * Flushes all of data buffered in both the currently active and the back
//	 * buffer into a new copy of the {@link List} that stores the actual data.
//	 * The original {@link List} is afterwards cleared to prevent items to be
//	 * written twice.
//	 * </p>
//	 * 
//	 * @return All available data stored in both buffers copied into a new
//	 *         {@link List}
//	 */
//	public List<T> flush();
}
