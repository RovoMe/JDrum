package at.rovo.caching.drum;

/**
 * <p>Defines the methods required by the dispatcher to handle responses of the caching system.</p>
 *
 * @param <V> The type of the value element
 * @param <A> The type of the auxiliary data element
 * 
 * @author Roman Vottner
 */
public interface IDispatcher<V extends ByteSerializer<V>, A extends ByteSerializer<A>>
{
	/**
	 * <p>Handles unique key check events</p>
	 * 
	 * @param key The key of the element
	 * @param aux The auxiliary data of the element
	 */
	public void uniqueKeyCheck(Long key, A aux);
	
	/**
	 * <p>Handles duplicate key check events</p>
	 * 
	 * @param key The key of the element
	 * @param value The value of the element
	 * @param aux The auxiliary data of the element
	 */
	public void duplicateKeyCheck(Long key, V value, A aux);
	
	/**
	 * <p>Handles unique key update events</p>
	 * 
	 * @param key The key of the element
	 * @param value The value of the element
	 * @param aux The auxiliary data of the element
	 */
	public void uniqueKeyUpdate(Long key, V value, A aux);
	
	/**
	 * <p>Handles duplicate key update events</p>
	 * 
	 * @param key The key of the element
	 * @param value The value of the element
	 * @param aux The auxiliary data of the element
	 */
	public void duplicateKeyUpdate(Long key, V value, A aux);
	
	/**
	 * <p>Handles update events</p>
	 * 
	 * @param key The key of the element
	 * @param value The value of the element
	 * @param aux The auxiliary data of the element
	 */
	public void update(Long key, V value, A aux);
}
