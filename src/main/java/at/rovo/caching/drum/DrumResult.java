package at.rovo.caching.drum;

/**
 * <p>Defines the result values for query-operations of the caching system. A query 
 * operation is either {@link DrumAction#CHECK} or {@link DrumAction#CHECK_UPDATE}.</p>
 * 
 * @author Roman Vottner
 */
public enum DrumResult 
{
	/**
	 * <p>Is returned by the caching system if the given key was not found in the 
	 * synchronized Berkeley DB</p>
	 */
	UNIQUE_KEY,
	/**
	 * <p>Is returned by the caching system if the given key was found in the 
	 * synchronized Berkeley DB</p>
	 */
	DUPLICATE_KEY
}
