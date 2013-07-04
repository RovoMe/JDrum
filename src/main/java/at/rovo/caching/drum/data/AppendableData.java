package at.rovo.caching.drum.data;

/**
 * <p>Marks an implementing class as being able to append data to its value
 * field.</p>
 * 
 * @author Roman Vottner
 *
 * @param <T>
 *            The type of the data the broker manages
 */
public interface AppendableData<T> extends ByteSerializer<T>
{
	/**
	 * <p>Appends data to the value field of this object.</p>
	 * 
	 * @param data The data to append to the value field of this object
	 */
	public void append(T data);
}
