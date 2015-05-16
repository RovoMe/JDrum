package at.rovo.caching.drum.data;

import java.io.Serializable;

/**
 * Marks any implementing object as being able to return its context as byte array.
 * <p>
 * The byte array should be return in big-endian.
 * <p>
 * This contract is necessary compared to {@link Serializable} as the <em>Serializable</em> does not provide the
 * possibility to take influence on the way objects are serialized or de-serialized.
 *
 * @param <T>
 *         The type of the data the broker manages
 *
 * @author Roman Vottner
 */
public interface ByteSerializer<T>
{
    /**
     * Returns the current context of this object as byte-array in big-endian order.
     *
     * @return The current context of this object as byte array
     */
    byte[] toBytes();

    /**
     * Reads a byte array and tries to convert these data into a valid instance of type <em>T</em>.
     *
     * @param data
     *         The byte array which should be transformed to an instance of type <em>T</em>
     *
     * @return The unmarshalled object of type T
     */
    T readBytes(byte[] data);
}
