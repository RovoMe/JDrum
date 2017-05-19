package at.rovo.drum.datastore;

import at.rovo.drum.DrumException;
import at.rovo.drum.DrumResult;
import at.rovo.drum.DrumStoreEntry;
import at.rovo.drum.NotAppendableException;
import java.io.Serializable;
import java.util.List;

/**
 * Common base class for data store specific {@link at.rovo.drum.Merger} implementations which helps to divide the
 * respective mergers to their own module.
 */
public abstract class DataStoreMerger<V extends Serializable, A extends Serializable>
{
    /** The name of the DRUM instance **/
    protected final String drumName;
    /** The actual type of a value entry **/
    protected final Class<V> valueClass;

    public DataStoreMerger(String drumName, Class<V> valueClass)
    {
        this.drumName = drumName;
        this.valueClass = valueClass;
    }


    /**
     * Checks the keys of data elements stored in the bucket buffer with keys already stored in the backing data store
     * and merges them into the data store if they need an update.
     * <p>
     * If the key is already present in the data store the result field of the data object will be set to {@link
     * DrumResult#DUPLICATE_KEY}, else to {@link DrumResult#UNIQUE_KEY}.
     *
     * @param data
     *         The list of data to check against the data in the data store
     * @return The number of unique entries
     */
    public abstract long compareDataWithDataStore(List<? extends DrumStoreEntry<V, A>> data)
            throws DrumException, NotAppendableException;

    /**
     * Sets the cursor of the data store back to the start.
     */
    public abstract void reset();

    /**
     * Requests to close any open file handles to the backing data store.
     */
    public abstract void close();
}
