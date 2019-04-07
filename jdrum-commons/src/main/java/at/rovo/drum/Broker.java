package at.rovo.drum;

import java.io.Serializable;
import java.util.Queue;

/**
 * The broker manages message exchange between a producer and a consumer.
 * <p>
 * New data is added to the broker via {@link #put(DrumStoreEntry)} and may be retrieved via {@link #takeAll()}.
 *
 * @param <T> The type of the data stored by the broker
 * @param <V> The type of the data object the broker will manage
 * @author Roman Vottner
 */
public interface Broker<T extends DrumStoreEntry<V, ?>, V extends Serializable> extends Stoppable {

    /**
     * Feeds the broker with a new data item.
     *
     * @param data The data item to add to the broker
     * @throws IllegalStateException Thrown if entries are added to the broker after a shutdown was requested
     */
    void put(T data);

    /**
     * Returns all data stored by the broker.
     *
     * @return The stored data of the broker instance
     */
    Queue<T> takeAll();
}
