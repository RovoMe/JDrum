package at.rovo.caching.drum;

import java.util.Comparator;

/**
 * <p>Compares the keys of two objects. If key1 is less than key2 <code>compare
 * </code> will return -1, 1 if key2 is bigger than key1 and 0 if they are both
 * equal.</p>
 * <p>Note that the objects to compare must provide a <code>getKey()</code> 
 * method which returns a {@link Comparable} object.</p>
 *
 * @param <T> The type of the objects to compare. Note that the object must
 *            implement a getKey() method which returns a {@link Comparable}
 *            object.
 * 
 * @author Roman Vottner
 */
public class KeyComparator<T extends InMemoryData<?,?>> implements Comparator<T>
{
	@Override
	public int compare(T o1, T o2) 
	{
		if (o1.getKey() < o2.getKey())
			return -1;
		else if (o1.getKey() > o2.getKey())
			return 1;
		return 0;
	}
}
