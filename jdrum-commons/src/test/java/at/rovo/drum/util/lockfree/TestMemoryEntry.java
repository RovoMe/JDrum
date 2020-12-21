package at.rovo.drum.util.lockfree;

import at.rovo.drum.DrumOperation;
import at.rovo.drum.DrumResult;
import at.rovo.drum.DrumStoreEntry;
import at.rovo.drum.NotAppendableException;
import at.rovo.drum.PositionAware;
import at.rovo.drum.data.AppendableData;
import at.rovo.drum.util.DrumUtils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.io.Serializable;

/**
 * <em>InMemoryData</em> is a bean which holds the data related to an object that either should be stored within the
 * DRUM cache or is returned due to a DUPLICATE event from a back-end data store.
 * <p>
 * Moreover, the bytes of the key, value or auxiliary data attached to a key can be retrieved via {@link #getKey()},
 * {@link #getValue()} or {@link #getAuxiliary()} methods.
 *
 * @param <V> The type of the value
 * @param <A> The type of the auxiliary data attached to a key
 * @author Roman Vottner
 */
public class TestMemoryEntry<V extends Serializable, A extends Serializable>
        extends PositionAware
        implements DrumStoreEntry<V, A> {

    /**
     * The key of the statement
     */
    private Long key;
    /**
     * The value belonging to the key
     */
    private V value;
    /**
     * Additional information related to the key
     */
    private A aux;
    /**
     * The DRUM operation to execute on the data
     */
    private final DrumOperation operation;
    /**
     * The result extracted based on the data and the executed DRUM operation
     */
    private DrumResult result = null;

    /**
     * Creates a new instance of this class and initializes required fields.
     *
     * @param key       The key of this data object
     * @param value     The value of this data object
     * @param aux       The auxiliary data attached to the key
     * @param operation The DRUM operation to execute for this data object
     */
    TestMemoryEntry(@Nonnull final Long key, @Nullable final V value, @Nullable final A aux, @Nonnull final DrumOperation operation) {
        this.key = key;
        this.value = value;
        this.aux = aux;
        this.operation = operation;
    }

    @Override
    @Nonnull
    public Long getKey() {
        return this.key;
    }

    @Override
    public void setKey(@Nonnull final Long key) {
        this.key = key;
    }

    @Override
    @Nullable
    public V getValue() {
        return this.value;
    }

    @Override
    public void setValue(@Nullable final V value) {
        this.value = value;
    }

    @Override
    @Nullable
    public A getAuxiliary() {
        return this.aux;
    }

    @Override
    public void setAuxiliary(@Nullable final A aux) {
        this.aux = aux;
    }

    @Override
    @Nonnull
    public DrumOperation getOperation() {
        return this.operation;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void appendValue(@Nonnull final V data) throws NotAppendableException {
        if (null != this.value) {
            if (this.value instanceof AppendableData) {
                ((AppendableData<V>) this.value).append(data);
            } else {
                throw new NotAppendableException(
                        "Value data does not implement at.rovo.caching.drum.AppendableData interface!");
            }
        } else {
            // in case the value was null before and data should be appended, the current value of this instance should
            // contain the appended value
            this.value = data;
        }
    }

    @Override
    @Nonnull
    public byte[] getKeyAsBytes() {
        return DrumUtils.long2bytes(key);
    }

    @Override
    @Nullable
    public byte[] getValueAsBytes() {
        if (this.value == null) {
            return null;
        }

        byte[] bytes;
        try {
            bytes = DrumUtils.serialize(this.value);
        } catch (IOException ioEx) {
            bytes = new byte[0];
        }
        return bytes;
    }

    /**
     * Returns the attached data to a key for this data object as a byte-array in big endian order.
     *
     * @return The attached data to a key as byte-array
     */
    @Override
    @Nullable
    public byte[] getAuxiliaryAsBytes() {
        if (this.aux == null) {
            return null;
        }

        byte[] bytes;
        try {
            bytes = DrumUtils.serialize(this.aux);
        } catch (IOException ioEx) {
            bytes = new byte[0];
        }
        return bytes;
    }

    @Override
    public void setResult(@Nonnull final DrumResult result) {
        this.result = result;
    }

    @Override
    @Nonnull
    public DrumResult getResult() {
        return this.result;
    }

    @Override
    public long getByteLengthKV() {
        // the 8 byte long key and the 4 byte long integer value representing the length of bytes the value field does
        // take
        long bytes = 12;
        if (this.value != null) {
            final byte[] valueBytes = this.getValueAsBytes();
            if (null != valueBytes) {
                bytes += this.getValueAsBytes().length;
            }
        }

        return bytes;
    }

    @Override
    public int hashCode() {
        int hash = 1;
        hash = 37 * hash + key.hashCode();
        hash = 37 * hash + (value != null ? value.hashCode() : 0);
        hash = 37 * hash + (aux != null ? aux.hashCode() : 0);
        hash = 37 * hash + operation.hashCode();
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
        if (other instanceof TestMemoryEntry) {
            @SuppressWarnings("unchecked")
            TestMemoryEntry<V, A> o = (TestMemoryEntry<V, A>) other;
            return this.key.equals(o.key)
                    && (this.value == null && o.value == null || this.value != null && this.value.equals(o.value))
                    && (this.aux == null && o.aux == null || this.aux != null && this.aux.equals(o.aux))
                    && this.operation.equals(o.operation);
        }
        return false;
    }

    @Override
    @Nonnull
    public String toString() {
        return "op: " + this.operation + " key: " + this.key + " value: " + this.value + " aux: " + this.aux +
                " result: " + this.result;
    }
}
