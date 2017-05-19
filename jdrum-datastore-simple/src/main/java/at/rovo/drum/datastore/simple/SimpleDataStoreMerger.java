package at.rovo.drum.datastore.simple;

import at.rovo.drum.DrumException;
import at.rovo.drum.DrumOperation;
import at.rovo.drum.DrumResult;
import at.rovo.drum.DrumStoreEntry;
import at.rovo.drum.NotAppendableException;
import at.rovo.drum.datastore.DataStoreMerger;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * <em>DataStoreMerger</em> uses {@link SimpleDataStoreImpl} to check keys for their uniqueness and merges data that needs to
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
public class SimpleDataStoreMerger<V extends Serializable, A extends Serializable> extends DataStoreMerger<V, A>
{
    /** The logger of this class **/
    private final static Logger LOG = LogManager.getLogger(SimpleDataStoreMerger.class);

    /** A reference to the actual data store **/
    private final SimpleDataStore<V> dataStore;

    /**
     * Creates a new instance and initializes required instance fields.
     *
     * @param drumName
     *         The name of the DRUM instance to merge data for
     * @param valueClass
     *         The actual type of a value entry
     *
     * @throws DrumException
     *         Thrown if the data store could not get initialized
     */
    public SimpleDataStoreMerger(String drumName, Class<V> valueClass) throws DrumException
    {
        super(drumName, valueClass);

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
        this.dataStore = new SimpleDataStoreImpl<>(drumDir + "/cache.db", drumName, valueClass);
    }

    @Override
    public long compareDataWithDataStore(List<? extends DrumStoreEntry<V, A>> data)
            throws DrumException, NotAppendableException
    {
        long numUniqueEntries = 0L;
        for (DrumStoreEntry<V, ?> element : data)
        {
            Long key = element.getKey();
            DrumOperation op = element.getOperation();

            // set the result for CHECK and CHECK_UPDATE operations for a certain key
            if (DrumOperation.CHECK.equals(op) || DrumOperation.CHECK_UPDATE.equals(op))
            {
                try
                {
                    DrumStoreEntry<V, ?> entry = this.dataStore.getEntry(key);
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
                    numUniqueEntries = this.dataStore.getNumberOfEntries();
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
                    numUniqueEntries = this.dataStore.getNumberOfEntries();
                }
                catch (IOException | InstantiationException | IllegalAccessException | ClassNotFoundException e)
                {
                    throw new DrumException("Error writing data object with key: " + key + "!", e);
                }
            }

            LOG.info("[{}] - synchronizing key: '{}' operation: '{}' with repository - result: '{}'", this.drumName,
                     key, op, element.getResult());
        }
        return numUniqueEntries;
    }

    @Override
    public void reset()
    {
        this.dataStore.reset();
    }

    @Override
    public void close()
    {
        if (this.dataStore != null)
        {
            try
            {
                this.dataStore.close();
            }
            catch (Exception e)
            {
                LOG.warn("Caught exception while closing data store: {}", e.getLocalizedMessage());
            }
        }
    }
}
