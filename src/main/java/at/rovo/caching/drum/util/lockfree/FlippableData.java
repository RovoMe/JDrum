package at.rovo.caching.drum.util.lockfree;

import at.rovo.caching.drum.internal.InMemoryEntry;
import java.util.Queue;

/**
 * The internal data object held by the container.
 *
 * @param <T>
 *         The type of the entry the internal {@link Queue} can hold
 *
 * @author Roman Vottner
 */
public class FlippableData<T extends InMemoryEntry>
{
    private final Queue<T> queue;
    private final int keyLength;
    private final int valLength;
    private final int auxLength;

    /**
     * Creates a new instance of a container data object.
     *
     * @param queue
     *         The {@link Queue} in memory entries will be added to
     * @param keyLength
     *         The current byte length of all key bytes stored within the backing queue
     * @param valLength
     *         The current byte length of all value bytes stored within the backing queue
     * @param auxLength
     *         The current byte length of all auxiliary data bytes stored within the backing queue
     */
    public FlippableData(Queue<T> queue, int keyLength, int valLength, int auxLength)
    {
        this.queue = queue;
        this.keyLength = keyLength;
        this.valLength = valLength;
        this.auxLength = auxLength;
    }

    /**
     * Returns a reference to the backing {@link Queue}.
     *
     * @return The backing queue holding the added {@link InMemoryEntry} entries
     */
    Queue<T> getQueue()
    {
        return this.queue;
    }

    /**
     * Returns the total byte length of all {@link InMemoryEntry} keys stored in the backing {@link Queue}.
     *
     * @return The total byte length of all stored keys
     */
    public int getKeyLength()
    {
        return this.keyLength;
    }

    /**
     * Returns the total byte length off all {@link InMemoryEntry} values stored in the backing {@link Queue}.
     *
     * @return The total byte length of all stored values
     */
    public int getValLength()
    {
        return this.valLength;
    }

    /**
     * Returns the total byte length of all {@link InMemoryEntry} auxiliary data bytes stored in the backing
     * {@link Queue}.
     *
     * @return The total byte length of all stored auxiliary data bytes
     */
    public int getAuxLength()
    {
        return this.auxLength;
    }
}
