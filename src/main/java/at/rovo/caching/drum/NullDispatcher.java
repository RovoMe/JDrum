package at.rovo.caching.drum;

/**
 * <p>The null dispatcher implements the null object design pattern that offers suitable 
 * default do nothing behavior for required methods. Its main purpose is to provide a
 * base class to extend from and only require limited methods overriding.</p>
 *
 * @param <V> The type of the value element
 * @param <A> The type of the auxiliary data element
 * 
 * @author Roman Vottner
 */
public class NullDispatcher<V extends ByteSerializer<V>, A extends ByteSerializer<A>> implements IDispatcher<V,A>
{
	/**
	 * <p>Handles unique key check events</p>
	 */
	public void uniqueKeyCheck(Long key, A aux) 
	{
		
	}

	/**
	 * <p>Handles duplicate key check events</p>
	 */
	public void duplicateKeyCheck(Long key, V value, A aux) 
	{
		
	}

	/**
	 * <p>Handles unique key update events</p>
	 */
	public void uniqueKeyUpdate(Long key, V value, A aux) 
	{
		
	}

	/**
	 * <p>Handles duplicate key update events</p>
	 */
	public void duplicateKeyUpdate(Long key, V value, A aux) 
	{
		
	}

	/**
	 * <p>Handles update events</p>
	 */
	public void update(Long key, V value, A aux) 
	{
		
	}
}
