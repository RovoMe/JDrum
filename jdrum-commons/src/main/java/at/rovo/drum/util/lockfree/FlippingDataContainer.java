package at.rovo.drum.util.lockfree;

import at.rovo.drum.DrumStoreEntry;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This class implements a lock-free flippable data container. Internally a {@link FlippingDataContainerEntry} object is maintained
 * through utilizing an {@link AtomicReference}. New {@link DrumStoreEntry} entries can be added via {@link
 * #put(DrumStoreEntry)} which will be added to a {@link Queue} which is hold by the {@link FlippingDataContainerEntry} object. On
 * invoking {@link #flip()}) the current {@link FlippingDataContainerEntry} object is retrieved and a new empty {@link FlippingDataContainerEntry}
 * object is stored within the hold reference.
 *
 * @param <E> The type of the element the queue stored inside the data object can hold. The actual type has to extend
 *            {@link DrumStoreEntry}
 * @author Roman Vottner
 */
public class FlippingDataContainer<E extends DrumStoreEntry> {
    private final AtomicReference<FlippingDataContainerEntry<E>> dataObj = new AtomicReference<>();

    /**
     * Creates a new instance of a flippable data container.
     */
    public FlippingDataContainer() {
        dataObj.set(new FlippingDataContainerEntry<>(new ConcurrentLinkedQueue<>(), 0, 0, 0));
    }

    /**
     * Adds a new entry to the backing queue held by the current {@link FlippingDataContainerEntry} object.
     *
     * @param value The entry to add to the container
     * @return The current version of the {@link FlippingDataContainerEntry} object the entry was added to
     */
    public FlippingDataContainerEntry<E> put(E value) {
        if (null != value) {
            while (true) {
                FlippingDataContainerEntry<E> data = dataObj.get();
                FlippingDataContainerEntry<E> merged = FlippingDataContainerEntry.from(data, value);
                if (dataObj.compareAndSet(data, merged)) {
                    return merged;
                }
            }
        }
        return null;
    }

    /**
     * Switches the current {@link FlippingDataContainerEntry} object with a new version and returns the {@link Queue} held by the
     * replaced {@link FlippingDataContainerEntry} object.
     *
     * @return The {@link Queue} stored within the removed {@link FlippingDataContainerEntry} object
     */
    public Queue<E> flip() {
        FlippingDataContainerEntry<E> oldData;
        FlippingDataContainerEntry<E> newData = new FlippingDataContainerEntry<>(new ConcurrentLinkedQueue<>(), 0, 0, 0);
        while (true) {
            oldData = dataObj.get();
            if (dataObj.compareAndSet(oldData, newData)) {
                return oldData.getQueue();
            }
        }
    }

    /**
     * Returns false if data is available within the current buffer, true if no further data is available.
     *
     * @return true if the current buffer is empty; false otherwise
     */
    public boolean isEmpty() {
        return dataObj.get().getQueue().isEmpty();
    }
}
