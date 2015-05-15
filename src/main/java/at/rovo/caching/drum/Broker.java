package at.rovo.caching.drum;

import at.rovo.caching.drum.data.ByteSerializer;
import at.rovo.caching.drum.internal.DrumRuntimeListener;
import at.rovo.caching.drum.internal.InMemoryData;

import java.util.List;

/**
 * The broker manages message exchange between a producer and a consumer.
 * <p>
 * New data is added to the broker via {@link #put(T)} and may be retrieved via {@link #takeAll()}.
 *
 * @param <T>
 * 		The type of the data stored by the broker
 *
 * @author Roman Vottner
 */
public interface Broker<T extends InMemoryData<V, A>, V extends ByteSerializer<V>, A extends ByteSerializer<A>>
		extends DrumRuntimeListener
{
	/**
	 * Feeds the broker with a new data item.
	 *
	 * @param data
	 * 		The data item to add to the broker
	 */
	void put(T data);

	/**
	 * Returns all data stored by the broker.
	 *
	 * @return The stored data of the broker instance
	 *
	 * @throws InterruptedException
	 */
	List<T> takeAll() throws InterruptedException;
}
