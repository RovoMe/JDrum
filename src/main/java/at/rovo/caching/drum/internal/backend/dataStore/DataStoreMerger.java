package at.rovo.caching.drum.internal.backend.dataStore;

import at.rovo.caching.drum.DrumStoreEntry;
import at.rovo.caching.drum.Dispatcher;
import at.rovo.caching.drum.DrumException;
import at.rovo.caching.drum.DrumOperation;
import at.rovo.caching.drum.DrumResult;
import at.rovo.caching.drum.NotAppendableException;
import at.rovo.caching.drum.DrumEventDispatcher;
import at.rovo.caching.drum.internal.DiskFileMerger;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * <em>DataStoreMerger</em> uses {@link DataStoreImpl} to check keys for their uniqueness and merges data that needs to
 * be updated with the cache file.
 * <p>
 * On successfully extracting the unique or duplicate status of an entry, the result will be injected into the
 * data-object itself to avoid loosing data information.
 *
 * @param <V>
 *         The type of the value
 * @param <A>
 *         The type of the auxiliary data
 *
 * @author Roman Vottner
 */
public class DataStoreMerger<V extends Serializable, A extends Serializable> extends DiskFileMerger<V, A>
{
    /** The logger of this class **/
    private final static Logger LOG = LogManager.getLogger(DataStoreMerger.class);

    /** A reference to the actual data store **/
    private DataStore<V> dataStore = null;

    /**
     * Creates a new instance and initializes required instance fields.
     *
     * @param drumName
     *         The name of the DRUM instance this cache is used for
     * @param dispatcher
     *         The dispatching instance to send results to
     */
    DataStoreMerger(String drumName, int numBuckets, Dispatcher<V, A> dispatcher, Class<V> valueClass,
                    Class<A> auxClass, DrumEventDispatcher eventDispatcher) throws DrumException
    {
        super(drumName, numBuckets, dispatcher, valueClass, auxClass, eventDispatcher);

        this.initCacheFile();
    }

    /**
     * Initializes and creates if necessary the directories and files for the cache file.
     * <p>
     * Note that the cache file will be placed inside a cache directory of the applications root-path in a further
     * directory which is named after the DRUM instance it is created for. The actual cache file is named <code>
     * cache.db</code>.
     *
     * @throws DrumException
     *         If the cache file could not be created
     */
    private void initCacheFile() throws DrumException
    {
        String appDir = System.getProperty("user.dir");
        File cacheDir = new File(appDir + "/cache");
        if (!cacheDir.exists())
        {
            boolean result = cacheDir.mkdir();
            if (!result)
            {
                LOG.warn("Could not create cache directory");
            }
        }
        File drumDir = new File(cacheDir + "/" + drumName);
        if (!drumDir.exists())
        {
            boolean result = drumDir.mkdir();
            if (!result)
            {
                LOG.warn("Could not create DRUM directory!");
            }
        }
        File cache = new File(drumDir + "/cache.db");
        if (!cache.exists())
        {
            try
            {
                boolean result = cache.createNewFile();
                if (!result)
                {
                    LOG.warn("");
                }
            }
            catch (IOException e)
            {
                throw new DrumException("Error while creating data store cache.db! Reason: " + e.getLocalizedMessage());
            }
        }
        this.dataStore = new DataStoreImpl<>(drumDir + "/cache.db", this.drumName, this.valueClass);
    }

    @Override
    protected void compareDataWithDataStore(List<? extends DrumStoreEntry<V>> data)
            throws DrumException, NotAppendableException
    {
        for (DrumStoreEntry<V> element : data)
        {
            Long key = element.getKey();
            DrumOperation op = element.getOperation();

            // set the result for CHECK and CHECK_UPDATE operations for a certain key
            if (DrumOperation.CHECK.equals(op) || DrumOperation.CHECK_UPDATE.equals(op))
            {
                try
                {
                    DrumStoreEntry<V> entry = this.dataStore.getEntry(key);
                    if (entry == null)
                    {
                        element.setResult(DrumResult.UNIQUE_KEY);
                    }
                    else
                    {
                        element.setResult(DrumResult.DUPLICATE_KEY);
                        // in case we have a duplicate check element set its value to the contained value. checkUpdates
                        // contains already the latest entry as the value will be written to the data store in the next
                        // section
                        if (DrumOperation.CHECK.equals(element.getOperation()))
                        {
                            element.setValue(entry.getValue());
                        }
                    }
                }
                catch (IOException | InstantiationException | IllegalAccessException | ClassNotFoundException e)
                {
                    throw new DrumException("Error retrieving data object with key: " + key + "!", e);
                }
            }

            // update the value of a certain key in case of UPDATE or CHECK_UPDATE operations within the bucket file
            if (DrumOperation.UPDATE.equals(op) || DrumOperation.CHECK_UPDATE.equals(op))
            {
                try
                {
                    this.dataStore.writeEntry(element, false);
                    this.numUniqueEntries = this.dataStore.getNumberOfEntries();
                }
                catch (IOException | InstantiationException | IllegalAccessException | ClassNotFoundException e)
                {
                    throw new DrumException("Error writing data object with key: " + key + "!", e);
                }
            }
            // data object contains request to append the data to the existing data in case the entry already exists -
            // if not it is created
            else if (DrumOperation.APPEND_UPDATE.equals(op))
            {
                try
                {
                    element.setValue(this.dataStore.writeEntry(element, true).getValue());
                    this.numUniqueEntries = this.dataStore.getNumberOfEntries();
                }
                catch (IOException | InstantiationException | IllegalAccessException | ClassNotFoundException e)
                {
                    throw new DrumException("Error writing data object with key: " + key + "!", e);
                }
            }

            LOG.info("[{}] - synchronizing key: '{}' operation: '{}' with repository - result: '{}'", this.drumName,
                     key, op, element.getResult());
        }
    }

    @Override
    public void reset()
    {
        this.dataStore.reset();
    }

    @Override
    public void close() throws Exception
    {
        if (this.dataStore != null)
        {
            this.dataStore.close();
        }
    }
}
