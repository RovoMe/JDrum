package at.rovo.drum.util.lockfree;

import at.rovo.drum.DrumStoreEntry;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * The internal data object held by the container.
 *
 * @param <T> The type of the entry the internal {@link Queue} can hold
 * @author Roman Vottner
 */
public class FlippingDataContainerEntry<T extends DrumStoreEntry<? extends Serializable, ? extends Serializable>> {

    private final Queue<T> queue;
    private final int keyLength;
    private final int valLength;
    private final int auxLength;

    /**
     * Creates a new instance of a container data object.
     *
     * @param queue     The {@link Queue} in memory entries will be added to
     * @param keyLength The current byte length of all key bytes stored within the backing queue
     * @param valLength The current byte length of all value bytes stored within the backing queue
     * @param auxLength The current byte length of all auxiliary data bytes stored within the backing queue
     */
    FlippingDataContainerEntry(@Nonnull final Queue<T> queue, final int keyLength, final int valLength, final int auxLength) {
        this.queue = queue;
        this.keyLength = keyLength;
        this.valLength = valLength;
        this.auxLength = auxLength;
    }

    /**
     * Create a new instance based on data of an existing instance of this class.
     * @param data      The flippable data object to copy data from
     * @param value     The value to add
     * @param <T>       The type the flippable data object holds
     * @return A new instance containing a merge of the provided object with the passed arguments
     */
    @Nonnull
    static <T extends DrumStoreEntry<? extends Serializable, ? extends Serializable>> FlippingDataContainerEntry<T> from(@Nonnull final FlippingDataContainerEntry<T> data, T value) {
        final Queue<T> queue = new ConcurrentLinkedQueue<>(data.getQueue());
        queue.add(value);
        return new FlippingDataContainerEntry<>(queue,
                data.getKeyLength() + value.getKeyAsBytes().length,
                data.getValLength() + (value.getValueAsBytes() != null ? value.getValueAsBytes().length : 0),
                + data.getAuxLength() + ( value.getAuxiliaryAsBytes() != null ? value.getAuxiliaryAsBytes().length : 0));
    }

    /**
     * Returns a reference to the backing {@link Queue}.
     *
     * @return The backing queue holding the added {@link DrumStoreEntry} entries
     */
    @Nonnull
    public Queue<T> getQueue() {
        return this.queue;
    }

    /**
     * Returns the total byte length of all {@link DrumStoreEntry} keys stored in the backing {@link Queue}.
     *
     * @return The total byte length of all stored keys
     */
    public int getKeyLength() {
        return this.keyLength;
    }

    /**
     * Returns the total byte length off all {@link DrumStoreEntry} values stored in the backing {@link Queue}.
     *
     * @return The total byte length of all stored values
     */
    public int getValLength() {
        return this.valLength;
    }

    /**
     * Returns the total byte length of all {@link DrumStoreEntry} auxiliary data bytes stored in the backing
     * {@link Queue}.
     *
     * @return The total byte length of all stored auxiliary data bytes
     */
    public int getAuxLength() {
        return this.auxLength;
    }

    @Override
    public int hashCode() {
        int hash = 1;
        hash = 37 * hash + queue.hashCode();
        hash = 37 * hash + keyLength;
        hash = 37 * hash + valLength;
        hash = 37 * hash + auxLength;
        return hash;
    }

    @Override
    public boolean equals(@Nullable final Object other) {
        if (other == this) {
            return true;
        }
        if (null == other) {
            return false;
        }
        if (other instanceof FlippingDataContainerEntry) {
            @SuppressWarnings("unchecked")
            FlippingDataContainerEntry<T> o = (FlippingDataContainerEntry<T>) other;
            return this.queue.equals(o.queue)
                    && this.keyLength == o.keyLength
                    && this.valLength == o.valLength
                    && this.auxLength == o.auxLength;
        }
        return false;
    }
}
