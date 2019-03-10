package at.rovo.drum;

import java.io.Serializable;

/**
 * This interface contains the required methods for the "Disc Repository with Update Management" (DRUM) implementation
 * as presented by Lee, Leonard, Wang and Loguinov in their paper "IRLbot: Scaling to 6 Billion Pages and Beyond"
 * <p>
 * DRUM is a local caching system where key/value pairs are kept in memory or on local disc files utilizing a bucket
 * sort strategy to minimize all lookups and modifications to the buckets. It therefore allows for efficient storage of
 * large collections of &lt;key, value> pairs, where key is a unique identifier (hash) of some data and value is the
 * arbitrary information attached to the key.
 * <p>
 * DRUM basically supports 3 types of operations: <ul> <li>check</li> <li>update</li> <li>check+update</li> </ul>
 * <b>check</b>: the incoming set of data contains keys that must be checked against those stored in the disk cache and
 * classified as being duplicate or unique. For duplicate keys, the value associated with each key can be optionally
 * retrieved from disk and used for some processing.
 * <p>
 * <b>update</b>: the incoming list contains &lt;key, value> pairs that need to be merged into the existing disk cache.
 * If a given key exists, its value is updated (e.g. overridden or incremented), if it does not, a new entry is created
 * in the disk file.
 * <p>
 * <b>check+update</b>: performs both check and update in one pass through the disk cache.
 * <p>
 * Note that this implementation is a spin-off based on the C++ implementation of Leandro T C Melo, but it was modified
 * heavily since the start of the implementation to better fit the needs. Those modifications include a further method
 * <b>appendUpdate</b> which appends the value in the back-end data store instead of replacing the value. Moreover does
 * this implementation support synchronization.
 *
 * @param <V> The type of the value to be stored in the cache
 * @param <A> The type of auxiliary data, which needs to be attached to key/value-pairs
 * @author Roman Vottner
 * @version 0.1
 * @link http://irl.cs.tamu.edu/people/hsin-tsang/papers/www2008.pdf
 */
public interface Drum<V extends Serializable, A extends Serializable> {

    /**
     * Informs the caching system to check for the availability of the provided key. There are only two options here:
     * The key can already be present therefore a {@link DrumResult#DUPLICATE_KEY} will be returned via the dispatcher,
     * otherwise {@link DrumResult#UNIQUE_KEY} is returned
     *
     * @param key The key which should be checked for.
     */
    void check(final Long key);

    /**
     * Informs the caching system to check for the availability of the provided key
     *
     * @param key The key which should be checked for.
     * @param aux The auxiliary data of a key.
     */
    void check(final Long key, final A aux);

    /**
     * Instructs the caching system to update the value of a certain key on the next merge phase. If the key is not
     * present in the cache yet, it will be created with the provided value.
     *
     * @param key   The key which value should be updated
     * @param value The new value of the key
     */
    void update(final Long key, final V value);

    /**
     * Instructs the caching system to update the value and/or auxiliary data of a certain key on the next merge phase.
     * If the key does not exist in the cache yet, if will be created with the provided value/auxiliary data. If the key
     * already exists the entry will be replaced by the new data entry.
     *
     * @param key   The key which value should be updated
     * @param value The new value of the key
     * @param aux   The new auxiliary data of the key
     */
    void update(final Long key, final V value, final A aux);

    /**
     * Instructs the caching system to update the value and/or auxiliary data of a certain key on the next merge phase.
     * If the key does not exist in the cache yet, if will be created with the provided value/auxiliary data. If the key
     * already exists the content of the new data will be added to the already existing entry instead of replacing the
     * content with the new version.
     *
     * @param key   The key which value should be updated
     * @param value The new value of the key
     */
    void appendUpdate(final Long key, final V value);

    /**
     * Instructs the caching system to update the value and/or auxiliary data of a certain key on the next merge phase.
     * If the key does not exist in the cache yet, if will be created with the provided value/auxiliary data. If the key
     * already exists the content of the new data will be added to the already existing entry instead of replacing the
     * content with the new version.
     *
     * @param key   The key which value should be updated
     * @param value The new value of the key
     * @param aux   The new auxiliary data of the key
     */
    void appendUpdate(final Long key, final V value, final A aux);

    /**
     * Executes the check and update operations in one single pass
     *
     * @param key   The key which existence should be checked and its associated data needs to be updated.
     * @param value The value of the key that needs to be updated.
     */
    void checkUpdate(final Long key, final V value);

    /**
     * Executes the check and update operations in one single pass
     *
     * @param key   The key which existence should be checked and its associated data needs to be updated.
     * @param value The value of the key that needs to be updated.
     * @param aux   The auxiliary data that needs to be updated.
     */
    void checkUpdate(final Long key, final V value, final A aux);

    /**
     * Releases the lock to the local backing DB and the locks held to other system imminent devices
     */
    void dispose() throws DrumException;
}
