package at.rovo.caching.drum.util.lockfree;

import at.rovo.caching.drum.internal.InMemoryData;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This class implements a lock-free flippable data container. Internally a {@link FlippableData} object is maintained
 * through utilizing an {@link AtomicReference}. New {@link InMemoryData} entries can be added via {@link
 * #put(InMemoryData)} which will be added to a {@link Queue} which is hold by the {@link FlippableData} object. On
 * invoking {@link #flip()}) the current {@link FlippableData} object is retrieved and a new empty {@link FlippableData}
 * object is stored within the hold reference.
 *
 * @param <E>
 *         The type of the element the queue stored inside the data object can hold. The actual type has to extend
 *         {@link InMemoryData}
 */
public class FlippableDataContainer<E extends InMemoryData>
{
    private final AtomicReference<FlippableData<E>> dataObj = new AtomicReference<>();

    /**
     * Creates a new instance of a flippable data container.
     */
    public FlippableDataContainer()
    {
        dataObj.set(new FlippableData<>(new ConcurrentLinkedQueue<>(), 0, 0, 0));
    }

    /**
     * Adds a new entry to the backing queue held by the current {@link FlippableData} object.
     *
     * @param value
     *         The entry to add to the container
     *
     * @return The current version of the {@link FlippableData} object the entry was added to
     */
    public FlippableData<E> put(E value)
    {
        if (null != value)
        {
            int keyLength = value.getKeyAsBytes() != null ? value.getKeyAsBytes().length : 0;
            int valLength = value.getValueAsBytes() != null ? value.getValueAsBytes().length : 0;
            int auxLength = value.getAuxiliaryAsBytes() != null ? value.getAuxiliaryAsBytes().length : 0;

            FlippableData<E> data = dataObj.get();
            while (true)
            {
                Queue<E> queue = data.getQueue();
                queue.add(value);
                FlippableData<E> merged =
                        new FlippableData<>(queue,
                                            data.getKeyLength() + keyLength,
                                            data.getValLength() + valLength,
                                            data.getAuxLength() + auxLength);
                if (dataObj.compareAndSet(data, merged))
                {
                    return merged;
                }
            }
        }
        return null;
    }

    /**
     * Switches the current {@link FlippableData} object with a new version and returns the {@link Queue} held by the
     * replaced {@link FlippableData} object.
     *
     * @return The {@link Queue} stored within the removed {@link FlippableData} object
     */
    public Queue<E> flip()
    {
        FlippableData<E> oldData;
        while (true)
        {
            oldData = dataObj.get();
            if (dataObj.compareAndSet(oldData, new FlippableData<>(new ConcurrentLinkedQueue<>(), 0, 0, 0)))
            {
                break;
            }
        }
        return oldData.getQueue();
    }
}