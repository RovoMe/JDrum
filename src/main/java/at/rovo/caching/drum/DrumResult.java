package at.rovo.caching.drum;

/**
 * Defines the result values for query-operations of the caching system. A query operation is either {@link
 * DrumOperation#CHECK} or {@link DrumOperation#CHECK_UPDATE}.
 *
 * @author Roman Vottner
 */
public enum DrumResult
{
	/**
	 * Is returned by the caching system if the given key was not found in the synchronized Berkeley DB
	 */
	UNIQUE_KEY,
	/**
	 * Is returned by the caching system if the given key was found in the synchronized Berkeley DB
	 */
	DUPLICATE_KEY
}
