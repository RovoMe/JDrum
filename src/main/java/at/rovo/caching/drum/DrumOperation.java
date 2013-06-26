package at.rovo.caching.drum;

/**
 * <p>Defines valid operations for the caching system.</p>
 * 
 * @author Roman Vottner
 */
public enum DrumOperation 
{
	/**
	 * <p>Defines that the cache should be checked against a key for its availability.</p>
	 * <p>If one is already available {@link DrumResult#DUPLICATE_KEY} should be 
	 * triggered, else {@link DrumResult#UNIQUE_KEY} has to be returned</p>
	 */
	CHECK,
	/**
	 * <p>Defines that a value for a cached key needs to be updated. If the key is not yet
	 * stored in the cache it should be created instead with the given value. If the key is
	 * present it will be replaced by the new entry.</p>
	 */
	UPDATE,
	/**
	 * <p>Marks a certain element to be {@link #CHECK}ed first and then {@link #UPDATE}d afterwards.</p>
	 */
	CHECK_UPDATE,
	/**
	 * <p>Defines that a value for a cached key needs to be updated. If the key is not yet
	 * stored in the cache it should be created instead with the given value. If the key is
	 * already present it will append the data of the new data element to the content of the
	 * already stored data entry instead of replacing the entry.</p>
	 */
	APPEND_UPDATE
}
