package at.rovo.caching.drum.internal.backend.berkeley;

import at.rovo.caching.drum.Dispatcher;
import at.rovo.caching.drum.DrumEventDispatcher;
import at.rovo.caching.drum.DrumException;
import at.rovo.caching.drum.DrumOperation;
import at.rovo.caching.drum.DrumResult;
import at.rovo.caching.drum.DrumStoreEntry;
import at.rovo.caching.drum.internal.DiskFileMerger;
import at.rovo.caching.drum.util.DrumUtils;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.ExceptionEvent;
import com.sleepycat.je.ExceptionListener;
import com.sleepycat.je.OperationStatus;
import java.io.File;
import java.io.Serializable;
import java.util.List;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * <em>BerkeleyDBStoreMerger</em> uses a backing Berkeley DB to check keys for their uniqueness and merges data that
 * needs to be updated with the cache file.
 * <p>
 * On successfully extracting the unique or duplicate status of an entry, the result will be injected into the
 * data-object itself to avoid losing data information.
 *
 * @param <V>
 *         The type of the value
 * @param <A>
 *         The type of the auxiliary data
 *
 * @author Roman Vottner
 */
public class BerkeleyDBStoreMerger<V extends Serializable, A extends Serializable>
        extends DiskFileMerger<V, A> implements ExceptionListener
{
    /** The logger of this class **/
    private final static Logger LOG = LogManager.getLogger(BerkeleyDBStoreMerger.class);

    /** Berkeley's environment object **/
    private Environment environment = null;
    /** The actual reference to the BerkeleyDB **/
    private Database berkeleyDB = null;

    /**
     * Creates a new instance which will take care of merging the data with database and calculating the unique or
     * duplicate key check.
     *
     * @param drumName
     *         The name of the DRUM instance to merge data for
     * @param numBuckets
     *         The number of bucket files used by the DRUM instance
     * @param dispatcher
     *         The dispatcher object to send unique or duplicate responses with
     * @param valueClass
     *         The actual type of a value entry
     * @param auxClass
     *         The actual type of an auxiliary data element
     * @param eventDispatcher
     *         The dispatcher object to send internal state updates to
     *
     * @throws DrumException
     *         Thrown if the database could not get initialized
     */
    BerkeleyDBStoreMerger(String drumName, int numBuckets, Dispatcher<V, A> dispatcher, Class<V> valueClass,
                          Class<A> auxClass, DrumEventDispatcher eventDispatcher) throws DrumException
    {
        super(drumName, numBuckets, dispatcher, valueClass, auxClass, eventDispatcher);

        LOG.debug("{} - creating berkeley db", this.drumName);
        try
        {
            EnvironmentConfig envConfig = new EnvironmentConfig();
            envConfig.setAllowCreate(true);
            envConfig.setExceptionListener(this);

            // check if the cache sub-directory exists - if not create one
            File cacheDir = new File(System.getProperty("user.dir") + "/cache");
            if (!cacheDir.exists())
            {
                boolean success = cacheDir.mkdir();
                if (!success)
                {
                    LOG.warn("Could not create cache directory");
                }
            }
            // check if a sub-directory inside the cache sub-directory exists that has the name of this instance
            // - if not create it
            File dbFile = new File(System.getProperty("user.dir") + "/cache/" + this.drumName);
            if (!dbFile.exists())
            {
                boolean success = dbFile.mkdir();
                if (!success)
                {
                    LOG.warn("Could not create backend data-store");
                }
            }
            // create the environment for the DB
            this.environment = new Environment(dbFile, envConfig);

            DatabaseConfig dbConfig = new DatabaseConfig();
            // allow the database to be created if non could be found
            dbConfig.setAllowCreate(true);
            // Sets the comparator for the key
            dbConfig.setBtreeComparator(new BTreeCompare());
            // writing to DB does not occur upon operation call - instead it is delayed as long as possible changes are
            // only guaranteed to be durable after the Database.sync() method got called or the database is properly
            // closed
            dbConfig.setDeferredWrite(true);

            this.berkeleyDB = this.environment.openDatabase(null, this.drumName + ".db", dbConfig);
        }
        catch (DatabaseException e)
        {
            throw new DrumException(this.drumName + " - Creating Berkeley DB failed!", e);
        }
    }

    /**
     * Catches exceptions thrown by the Berkeley DB, which is used for storing the key/value and auxiliary data in disk
     * buckets, and forwards them to the application which uses DRUM
     */
    @Override
    public void exceptionThrown(ExceptionEvent exEvent)
    {
        LOG.error("{} - Berkeley DB Exception!", this.drumName);
        LOG.catching(Level.ERROR, exEvent.getException());
    }

    @Override
    public void close()
    {
        if (null != this.berkeleyDB)
        {
            this.berkeleyDB.close();
        }
        if (null != this.environment)
        {
            this.environment.close();
        }
    }

    @Override
    protected void compareDataWithDataStore(List<? extends DrumStoreEntry<V>> data) throws DrumException
    {
        try
        {
            for (DrumStoreEntry<V> element : data)
            {
                Long key = element.getKey();

                DatabaseEntry dbKey = new DatabaseEntry(DrumUtils.long2bytes(key));
                DrumOperation op = element.getOperation();

                // set the result for CHECK and CHECK_UPDATE operations for a certain key
                if (DrumOperation.CHECK.equals(op) || DrumOperation.CHECK_UPDATE.equals(op))
                {
                    // In Berkeley DB Java edition there is no method available to check for existence only so checking
                    // the key also retrieves the data
                    DatabaseEntry dbValue = new DatabaseEntry();
                    OperationStatus status = this.berkeleyDB.get(null, dbKey, dbValue, null);
                    if (OperationStatus.NOTFOUND.equals(status))
                    {
                        element.setResult(DrumResult.UNIQUE_KEY);
                    }
                    else
                    {
                        element.setResult(DrumResult.DUPLICATE_KEY);

                        if (DrumOperation.CHECK.equals(element.getOperation()) && dbValue.getData().length > 0)
                        {
                            V value = DrumUtils.deserialize(dbValue.getData(), this.valueClass);
                            element.setValue(value);
                        }
                    }
                }

                // update the value of a certain key in case of UPDATE or CHECK_UPDATE operations within the bucket file
                if (DrumOperation.UPDATE.equals(op) || DrumOperation.CHECK_UPDATE.equals(op))
                {
                    V value = element.getValue();
                    byte[] byteValue = DrumUtils.serialize(value);
                    DatabaseEntry dbValue = new DatabaseEntry(byteValue);
                    // forces overwrite if the key is already present
                    OperationStatus status = this.berkeleyDB.put(null, dbKey, dbValue);
                    if (!OperationStatus.SUCCESS.equals(status))
                    {
                        throw new DrumException("Error merging with repository!");
                    }
                }
                else if (DrumOperation.APPEND_UPDATE.equals(op))
                {
                    // read the old value and append it to the current value
                    DatabaseEntry dbReadValue = new DatabaseEntry();
                    OperationStatus status = this.berkeleyDB.get(null, dbKey, dbReadValue, null);
                    if (OperationStatus.KEYEXIST.equals(status))
                    {
                        V storedVal = DrumUtils.deserialize(dbReadValue.getData(), this.valueClass);
                        element.appendValue(storedVal);
                    }
                    // now write it to the DB replacing the old value
                    V value = element.getValue();
                    byte[] byteValue = DrumUtils.serialize(value);
                    DatabaseEntry dbWriteValue = new DatabaseEntry(byteValue);
                    // forces overwrite if the key is already present
                    status =  this.berkeleyDB.put(null, dbKey, dbWriteValue);

                    if (!OperationStatus.SUCCESS.equals(status))
                    {
                        throw new DrumException("Error merging with repository!");
                    }
                }

                LOG.info("[{}] - synchronizing key: '{}' operation: '{}' with repository - result: '{}'",
                         this.drumName, key, op, element.getResult());

                // Persist modifications
                this.berkeleyDB.sync();

                this.numUniqueEntries = this.berkeleyDB.count();
            }
        }
        catch (Exception e)
        {
            throw new DrumException("Error synchronizing buckets with repository!", e);
        }
    }

    @Override
    protected void reset()
    {
        // can be ignored
    }
}
