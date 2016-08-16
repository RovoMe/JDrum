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
import com.sleepycat.je.ThreadInterruptedException;
import java.io.File;
import java.io.Serializable;
import java.util.List;
import java.util.concurrent.Callable;
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

    /** A reference to the BerkeleyDB directory **/
    private final File dbFile;
    /** Berkeley's environment configuration**/
    private final EnvironmentConfig envConfig;
    /** Berkeley's database configuration **/
    private final DatabaseConfig dbConfig;
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
            this.envConfig = new EnvironmentConfig();
            this.envConfig.setAllowCreate(true);
            this.envConfig.setExceptionListener(this);

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
            this.dbFile = new File(System.getProperty("user.dir") + "/cache/" + this.drumName);
            if (!this.dbFile.exists())
            {
                boolean success = this.dbFile.mkdir();
                if (!success)
                {
                    LOG.warn("Could not create backend data-store");
                }
            }
            // create the environment for the DB
            this.environment = new Environment(this.dbFile, this.envConfig);

            this.dbConfig = new DatabaseConfig();
            // allow the database to be created if non could be found
            this.dbConfig.setAllowCreate(true);
            // Sets the comparator for the key
            this.dbConfig.setBtreeComparator(new BTreeCompare());
            // writing to DB does not occur upon operation call - instead it is delayed as long as possible changes are
            // only guaranteed to be durable after the Database.sync() method got called or the database is properly
            // closed
            this.dbConfig.setDeferredWrite(true);

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
                    OperationStatus status = tryCommand(() -> this.berkeleyDB.get(null, dbKey, dbValue, null));
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
                    OperationStatus status = tryCommand(() -> this.berkeleyDB.put(null, dbKey, dbValue));
                    if (!OperationStatus.SUCCESS.equals(status))
                    {
                        throw new DrumException("Error merging with repository!");
                    }
                }
                else if (DrumOperation.APPEND_UPDATE.equals(op))
                {
                    // read the old value and append it to the current value
                    DatabaseEntry dbReadValue = new DatabaseEntry();
                    OperationStatus status = tryCommand(() -> this.berkeleyDB.get(null, dbKey, dbReadValue, null));
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
                    status = tryCommand(() -> this.berkeleyDB.put(null, dbKey, dbWriteValue));

                    if (!OperationStatus.SUCCESS.equals(status))
                    {
                        throw new DrumException("Error merging with repository!");
                    }
                }

                LOG.info("[{}] - synchronizing key: '{}' operation: '{}' with repository - result: '{}'",
                         this.drumName, key, op, element.getResult());

                // Persist modifications
                tryCommand(this.berkeleyDB::sync);

                this.numUniqueEntries = tryCommand(this.berkeleyDB::count);
            }
        }
        catch (Exception e)
        {
            throw new DrumException("Error synchronizing buckets with repository!", e);
        }
    }

    /**
     * Tries to execute the provided work object. In case of a {@link ThreadInterruptedException}, which is thrown by
     * the underlying database layer, the database is re-initialized and the previously failed method is re-executed
     * again. If the error remains or an other error then {@link ThreadInterruptedException} is thrown, this error will
     * be propagated back to the invoking method.
     *
     * @param work
     *         The work load to execute
     */
    private void tryCommand(Runnable work)
    {
        try
        {
            work.run();
        }
        catch (ThreadInterruptedException tiEx)
        {
            LOG.warn("BerkeleyDB process was interrupted. Trying to reinitialize database");
            this.reinitializeDB();
            work.run();
        }
    }

    /**
     * Tries to execute the provided work object and returns its outcome. In case of a {@link
     * ThreadInterruptedException}, which is thrown by the underlying database layer, the database is re-initialized and
     * the previously failed method is re-executed again. In case the error could be solved, the outcome of the method
     * is returned, otherwise the failure is propagated to the invoking method.
     *
     * @param work
     *         The callable work which should be probed and in case of a {@link ThreadInterruptedException} retried with
     *         a re-initialized database
     * @param <T>
     *         The type of the return value
     *
     * @return The result of the provided callable argument
     *
     * @throws Exception
     *         In case an other exception then {@link ThreadInterruptedException} is thrown, this exception will be
     *         propagated. In case the re-tried work load has thrown the {@link ThreadInterruptedException} again, this
     *         exception will also be propagated back to the invoking method
     */
    private <T> T tryCommand(Callable<T> work) throws Exception
    {
        try
        {
            return work.call();
        }
        catch (ThreadInterruptedException tiEx)
        {
            LOG.warn("BerkeleyDB process was interrupted. Trying to reinitialize database");
            this.reinitializeDB();
            return work.call();
        }
    }

    /**
     * Re-initializes the BerkeleyDB in case of an {@link ThreadInterruptedException}.
     */
    private void reinitializeDB()
    {
        this.close();
        this.environment = new Environment(this.dbFile, this.envConfig);
        this.berkeleyDB = this.environment.openDatabase(null, this.drumName + ".db", dbConfig);
    }

    @Override
    protected void reset()
    {
        // can be ignored
    }
}
