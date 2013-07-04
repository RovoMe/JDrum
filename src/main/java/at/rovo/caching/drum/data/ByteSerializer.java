package at.rovo.caching.drum.data;

import java.io.Serializable;

/**
 * <p>
 * Marks any implementing object as being able to return its context as byte
 * array.
 * </p>
 * <p>
 * The byte array should be return in big-endian.
 * </p>
 * <p>
 * This contract is necessary compared to {@link Serializable} as the they don't
 * provide the possibility to take influence on the way objects are serialized
 * or de-serialized.
 * </p>
 * 
 * @author Roman Vottner
 * 
 * @param <T>
 *            The type of the data the broker manages
 */
public interface ByteSerializer<T>
{
	/**
	 * <p>
	 * Returns the current context of this object as byte-array in big-endian
	 * order.
	 * </p>
	 * 
	 * @return The current context of this object as byte array
	 */
	public byte[] toBytes();

	/**
	 * <p>
	 * Reads a byte array and tries to convert these data into a valid instance
	 * of type <em>T</em>.
	 * </p>
	 * 
	 * @param data
	 *            The byte array which should be transformed to an instance of
	 *            type <em>T</em>
	 * @return The unmarshaled object of type T
	 */
	public T readBytes(byte[] data);
}
