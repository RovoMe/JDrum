package at.rovo.caching.drum;

import at.rovo.caching.drum.data.AppendableData;
import java.io.Serializable;

/**
 * <em>DRUM</em> will compare in memory entries with entries in a backing data store and insert them if they are not yet
 * available within the data store. A class which is implementing this interface is therefore eligible for getting
 * persisted into and fetched from the DRUM data store.
 *
 * @author Roman Vottner
 */
public interface DrumStoreEntry<V extends Serializable> extends Serializable
{
    /**
     * Returns the key of the current data object.
     *
     * @return The key of the data object
     */
    Long getKey();

    /**
     * Sets the key of the current data object
     *
     * @param key
     *         The new key of the data object
     */
    void setKey(Long key);

    /**
     * Returns the value of the current data object.
     *
     * @return The value object related to the key of this data object
     */
    V getValue();

    /**
     * Sets the value of the current data object.
     *
     * @param value
     *         The new value of the data object
     */
    void setValue(V value);

    /**
     * Returns the DRUM operation to be used on this data object.
     *
     * @return The DRUM operation to be used
     */
    DrumOperation getOperation();

    /**
     * Appends the value of the provided data object to the value of this data object.
     *
     * @param data
     *         The data object containing the value to append to the value of this data object
     *
     * @throws IllegalArgumentException
     *         thrown if the provided data to append is null
     * @throws NotAppendableException
     *         Thrown if the data object to append is not an instance of {@link AppendableData}
     */
    void appendValue(V data) throws NotAppendableException;

    /**
     * Returns the key of this data object as an 8 byte long array in big-endian order.
     *
     * @return The key of this data object as byte-array
     */
    byte[] getKeyAsBytes();

    /**
     * Returns the value of this data object as a byte-array in big endian order.
     *
     * @return The value of this data object as byte-array
     */
    byte[] getValueAsBytes();

    /**
     * Sets the result for the defined DRUM operation based on the data of this object.
     *
     * @param result
     *         The result for this data object
     */
    void setResult(DrumResult result);

    /**
     * Returns the result gathered by DRUM based on the provided data object.
     *
     * @return The result for this data object
     */
    DrumResult getResult();

    /**
     * Returns the length of the key and the value in the byte-array.
     *
     * @return The length of the key and value in the byte-array
     */
    long getByteLengthKV();
}
