package at.rovo.caching.drum.internal;

import at.rovo.caching.drum.DrumOperation;
import at.rovo.caching.drum.DrumResult;
import at.rovo.caching.drum.DrumStoreEntry;
import at.rovo.caching.drum.NotAppendableException;
import at.rovo.caching.drum.data.AppendableData;
import at.rovo.caching.drum.util.DrumUtils;
import java.io.IOException;
import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * <em>InMemoryData</em> is a bean which holds the data related to an object that either should be stored within the
 * DRUM cache or is returned due to a DUPLICATE event from a back-end data store.
 * <p>
 * Besides getter and setter methods for the corresponding fields it provides a convenience method to restore the
 * original ordering after a sorting through {@link #getPosition()}.
 * <p>
 * Moreover, the bytes of the key, value or auxiliary data attached to a key can be retrieved via {@link #getKey()},
 * {@link #getValue()} or {@link #getAuxiliary()} methods.
 *
 * @param <V>
 *         The type of the value
 * @param <A>
 *         The type of the auxiliary data attached to a key
 *
 * @author Roman Vottner
 */
public class InMemoryEntry<V extends Serializable, A extends Serializable> implements DrumStoreEntry<V>
{
    /** The logger of this class **/
    private final static Logger LOG = LogManager.getLogger(MethodHandles.lookup().lookupClass());

    /** The key of the statement **/
    private Long key;
    /** The value belonging to the key **/
    private V value;
    /** Additional information related to the key **/
    private A aux;
    /** The DRUM operation to execute on the data **/
    private DrumOperation operation;
    /**
     * Will keep track of the original position of this data object before sorting to revert the sorting after the
     * key/value data was persisted in the bucket file
     **/
    private int position = 0;
    /**
     * The result extracted based on the data and the executed DRUM operation
     **/
    private DrumResult result = null;

    /**
     * Creates a new instance of this class without instantiating any field.
     */
    public InMemoryEntry()
    {

    }

    /**
     * Creates a new instance of this class and initializes required fields.
     *
     * @param key
     *         The key of this data object
     * @param value
     *         The value of this data object
     * @param aux
     *         The auxiliary data attached to the key
     * @param operation
     *         The DRUM operation to execute for this data object
     */
    public InMemoryEntry(Long key, V value, A aux, DrumOperation operation)
    {
        this.key = key;
        this.value = value;
        this.aux = aux;
        this.operation = operation;
    }

    @Override
    public Long getKey()
    {
        return this.key;
    }

    @Override
    public void setKey(Long key)
    {
        this.key = key;
    }

    @Override
    public V getValue()
    {
        return this.value;
    }

    @Override
    public void setValue(V value)
    {
        this.value = value;
    }

    /**
     * Returns the additional information attached to a key for the current data object.
     *
     * @return The additional information of the data object
     */
    public A getAuxiliary()
    {
        return this.aux;
    }

    /**
     * Sets the additional information attached to a key for the current data object.
     *
     * @param aux
     *         The additional information of the data object
     */
    public void setAuxiliary(A aux)
    {
        this.aux = aux;
    }

    @Override
    public DrumOperation getOperation()
    {
        return this.operation;
    }

    /**
     * Sets the DRUM operation to be used on this data object.
     *
     * @param operation
     *         The DRUM operation to be used
     */
    public void setOperation(DrumOperation operation)
    {
        this.operation = operation;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void appendValue(V data) throws NotAppendableException
    {
        if (data == null)
        {
            throw new IllegalArgumentException("Cannot append null value");
        }

        if (null != this.value)
        {
            if(this.value instanceof AppendableData)
            {
                ((AppendableData<V>) this.value).append(data);
            }
            else
            {
                throw new NotAppendableException(
                        "Value data does not implement at.rovo.caching.drum.AppendableData interface!");
            }
        }
        else
        {
            // in case the value was null before and data should be appended, the current value of this instance should
            // contain the appended value
            this.value = data;
        }
    }

    @Override
    public byte[] getKeyAsBytes()
    {
        return DrumUtils.long2bytes(key);
    }

    @Override
    public byte[] getValueAsBytes()
    {
        if (this.value == null)
        {
            return null;
        }

        byte[] bytes;
        try
        {
            bytes = DrumUtils.serialize(this.value);
        }
        catch (IOException ioEx)
        {
            LOG.warn("Could not turn value {} into a byte array for entry with key {}", this.value, this.key);
            bytes = new byte[0];
        }
        return bytes;
    }

    /**
     * Returns the attached data to a key for this data object as a byte-array in big endian order.
     *
     * @return The attached data to a key as byte-array
     */
    public byte[] getAuxiliaryAsBytes()
    {
        if (this.aux == null)
        {
            return null;
        }

        byte[] bytes;
        try
        {
            bytes = DrumUtils.serialize(this.aux);
        }
        catch (IOException ioEx)
        {
            LOG.warn("Could not turn auxiliary data entry {} into a byte array for entry with key {}",
                     this.aux, this.key);
            bytes = new byte[0];
        }
        return bytes;
    }

    /**
     * Intended to store the original position in the array managed by {@link DiskFileMerger} to enable reverting the
     * sorting after storing the key/value pair into the bucket file.
     *
     * @param position
     *         The original position for this data object before sorting
     */
    void setPosition(int position)
    {
        this.position = position;
    }

    /**
     * Returns the original position of this data object before the sorting happened in the <em>requestMerge()</em> method of
     * {@link DiskFileMerger}.
     *
     * @return The original position of this data object
     */
    int getPosition()
    {
        return this.position;
    }

    @Override
    public void setResult(DrumResult result)
    {
        this.result = result;
    }

    @Override
    public DrumResult getResult()
    {
        return this.result;
    }

    @Override
    public long getByteLengthKV()
    {
        // the 8 byte long key and the 4 byte long integer value representing the length of bytes the value field does
        // take
        long bytes = 12;
        if (this.value != null)
        {
            bytes += this.getValueAsBytes().length;
        }

        return bytes;
    }

    @Override
    public String toString()
    {
        return "op: " + this.operation + " key: " + this.key + " value: " + this.value + " aux: " + this.aux +
               " result: " + this.result;
    }
}
