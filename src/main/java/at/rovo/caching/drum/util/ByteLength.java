package at.rovo.caching.drum.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * <p>
 * <em>ByteLength</em> is a simple helper class which keeps track on the
 * byte-length of the lists added to this instance.
 * </p>
 * <p>
 * This is necessary to avoid mutable keys in {@link Map}s as they return null
 * if the <code>hashCode</code> changes, which happens on f.e. adding new
 * entries to a {@link List}.
 * </p>
 * 
 * @author Roman Vottner
 * 
 * @param <T>
 *            The type of the data the broker manages
 */
public class ByteLength<T>
{
	/** The list containing the byte data to keep track of **/
	private List<List<T>> list = null;
	/** The actual byte size stored for each added list **/
	private List<Integer> bytes = null;

	/**
	 * <p>
	 * Creates a new instance and instantiates required instance fields.
	 * </p>
	 */
	public ByteLength()
	{
		this.list = Collections.synchronizedList(new ArrayList<List<T>>());
		this.bytes = Collections.synchronizedList(new ArrayList<Integer>());
	}

	/**
	 * <p>
	 * Adds a new integer value associated with the provided {@link List}. If
	 * the list is not known to the object it will be added to the internal
	 * structure.
	 * </p>
	 * 
	 * @param list
	 *            The {@link List} object which should work as a key for the
	 *            <code>val</code>-value
	 * @param val
	 *            The value which should be associated with the {@link List}
	 */
	public synchronized void set(List<T> list, int val)
	{
		if (!this.list.contains(list))
		{
			this.list.add(list);
			this.bytes.add(val);
		}
		else
		{
			int index = this.list.indexOf(list);
			this.bytes.set(index, val);
		}
	}

	/**
	 * <p>
	 * Returns the value associated with the provided {@link List}.
	 * </p>
	 * 
	 * @param list
	 *            The {@link List} object whose associated value should be
	 *            returned
	 * @return The associated value for the provided {@link List}, 0 if no
	 *         association could be found
	 */
	public synchronized Integer get(List<T> list)
	{
		int index = this.list.indexOf(list);
		if (index != -1)
			return this.bytes.get(index);
		return 0;
	}
}
