package at.rovo.caching.drum.internal.backend.berkeley;

import java.io.File;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import at.rovo.caching.drum.DrumException;
import at.rovo.caching.drum.DrumOperation;
import at.rovo.caching.drum.DrumResult;
import at.rovo.caching.drum.IDispatcher;
import at.rovo.caching.drum.data.ByteSerializer;
import at.rovo.caching.drum.event.DrumEventDispatcher;
import at.rovo.caching.drum.internal.DiskFileMerger;
import at.rovo.caching.drum.internal.InMemoryData;
import at.rovo.caching.drum.util.DrumUtil;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.ExceptionEvent;
import com.sleepycat.je.ExceptionListener;
import com.sleepycat.je.OperationStatus;

/**
 * <p>
 * <em>BerkeleyCacheFileMerger</em> uses a backing Berkeley DB to check keys for
 * their uniqueness and merges data that needs to be updated with the cache
 * file.
 * </p>
 * <p>
 * On successfully extracting the unique or duplicate status of an entry, the
 * result will be injected into the data-object itself to avoid loosing data
 * informations.
 * </p>
 * 
 * @param <V>
 *            The type of the value
 * @param <A>
 *            The type of the auxiliary data
 * 
 * @author Roman Vottner
 */
public class BerkeleyCacheFileMerger<V extends ByteSerializer<V>, A extends ByteSerializer<A>>
		extends DiskFileMerger<V, A> implements ExceptionListener
{
	private final static Logger logger = LogManager
			.getLogger(BerkeleyCacheFileMerger.class);

	private Environment environment = null;
	private Database berkeleyDB;

	public BerkeleyCacheFileMerger(String drumName, int numBuckets,
			IDispatcher<V, A> dispatcher, Class<V> valueClass,
			Class<A> auxClass, DrumEventDispatcher eventDispatcher)
			throws DrumException
	{
		super(drumName, numBuckets, dispatcher, valueClass, auxClass,
				eventDispatcher);

		this.berkeleyDB = this.createDatabase(0);
	}

	/**
	 * <p>
	 * Creates the backing Berkeley DB with available memory to it in bytes.
	 * </p>
	 * 
	 * @param cacheSize
	 *            The available memory granted to the Berkeley DB
	 * @return The instance of the Berkeley DB object
	 * @throws DrumException
	 */
	private Database createDatabase(int cacheSize) throws DrumException
	{
		logger.debug("{} - creating berkeley db", this.drumName);
		try
		{
			EnvironmentConfig config = new EnvironmentConfig();
			config.setAllowCreate(true);
			config.setExceptionListener(this);
			if (cacheSize > 0)
				config.setCacheSize(cacheSize);

			// check if the cache sub-directory exists - if not create one
			File cacheDir = new File(System.getProperty("user.dir") + "/cache");
			if (!cacheDir.exists())
				cacheDir.mkdir();
			// check if a sub-directory inside the cache sub-directory exists
			// that has the
			// name of this instance - if not create it
			File file = new File(System.getProperty("user.dir") + "/cache/"
					+ this.drumName);
			if (!file.exists())
				file.mkdir();
			// create the environment for the DB
			this.environment = new Environment(file, config);

			DatabaseConfig dbConfig = new DatabaseConfig();
			// allow the database to be created if non could be found
			dbConfig.setAllowCreate(true);
			// Sets the comparator for the key
			dbConfig.setBtreeComparator(new BTreeCompare());
			// writing to DB does not occur upon operation call - instead it is
			// delayed as long as possible
			// changes are only guaranteed to be durable after the
			// Database.sync() method got called or the
			// database is properly closed
			dbConfig.setDeferredWrite(true);

			return this.environment.openDatabase(null, this.drumName + ".db",
					dbConfig);
		}
		catch (DatabaseException e)
		{
			logger.error("{} - Creating Berkeley DB failed!", this.drumName); 
			logger.catching(e);
			throw new DrumException(this.drumName
					+ " - Creating Berkeley DB failed!", e);
		}
	}

	/**
	 * Catches exceptions thrown by the Berkeley DB, which is used for storing
	 * the key/value and auxiliary data in disk buckets, and forwards them to
	 * the application which uses DRUM
	 */
	@Override
	public void exceptionThrown(ExceptionEvent exEvent)
	{
		logger.error("{} - Berkeley DB Exception!", this.drumName, exEvent.getException()); 
		logger.catching(exEvent.getException());
	}

	@Override
	public void close()
	{
		if (this.berkeleyDB != null)
			this.berkeleyDB.close();
		if (this.environment != null)
			this.environment.close();
	}

	@Override
	protected void compareDataWithDataStore(
			List<? extends InMemoryData<V, A>> data) throws DrumException
	{
		try
		{
			for (InMemoryData<V, A> element : data)
			{
				Long key = element.getKey();

				DatabaseEntry dbKey = new DatabaseEntry(DrumUtil
						.long2bytes(key));
				DrumOperation op = element.getOperation();

				assert ((DrumOperation.CHECK.equals(op)
						|| DrumOperation.CHECK_UPDATE.equals(op) 
						|| DrumOperation.UPDATE.equals(op)) 
						|| DrumOperation.APPEND_UPDATE.equals(op));

				// set the result for CHECK and CHECK_UPDATE operations for a
				// certain key
				if (DrumOperation.CHECK.equals(op)
						|| DrumOperation.CHECK_UPDATE.equals(op))
				{
					// In Berkeley DB Java edition there is no method available
					// to check for existence only
					// so checking the key also retrieves the data
					DatabaseEntry dbValue = new DatabaseEntry();
					OperationStatus status = this.berkeleyDB.get(null, dbKey, dbValue, null);
					if (OperationStatus.NOTFOUND.equals(status))
						element.setResult(DrumResult.UNIQUE_KEY);
					else
					{
						element.setResult(DrumResult.DUPLICATE_KEY);

						if (DrumOperation.CHECK.equals(element.getOperation())
								&& dbValue.getData().length > 0)
						{
							V value = DrumUtil.deserialize(dbValue.getData(), this.valueClass);
							element.setValue(value);
						}
					}
				}

				// update the value of a certain key in case of UPDATE or
				// CHECK_UPDATE operations
				// within the bucket file
				if (DrumOperation.UPDATE.equals(op)
						|| DrumOperation.CHECK_UPDATE.equals(op))
				{
					V value = element.getValue();
					byte[] byteValue = DrumUtil.serialize(value);
					DatabaseEntry dbValue = new DatabaseEntry(byteValue);
					OperationStatus status = this.berkeleyDB.put(null, dbKey,
							dbValue); // forces overwrite if the key is already
										// present
					if (!OperationStatus.SUCCESS.equals(status))
						throw new DrumException("Error merging with repository!");
				}
				else if (DrumOperation.APPEND_UPDATE.equals(op))
				{
					// read the old value and append it to the current value
					DatabaseEntry dbValue = new DatabaseEntry();
					OperationStatus status = this.berkeleyDB.get(null, dbKey, dbValue, null);
					if (OperationStatus.KEYEXIST.equals(status))
					{
						V value = element.getValue();
						V storedVal = value.readBytes(dbValue.getData());
						element.appendValue(storedVal);
					}
					// now write it to the DB replacing the old value
					V value = element.getValue();
					byte[] byteValue = DrumUtil.serialize(value);
					dbValue = new DatabaseEntry(byteValue);
					// forces overwrite if the key is already present
					status = this.berkeleyDB.put(null, dbKey, dbValue); 
					
					if (!OperationStatus.SUCCESS.equals(status))
						throw new DrumException("Error merging with repository!");
				}

				logger.info("[{}] - synchronizing key: '{}' operation: '{}' with repository - result: '{}'", 
						this.drumName, key, op, element.getResult());

				// Persist modifications
				this.berkeleyDB.sync();

				this.numUniqueEntries = this.berkeleyDB.count();
			}
		}
		catch (Exception e)
		{
			logger.error("[{}] - Error synchronizing buckets with repository!",
					this.drumName); 
			logger.catching(e);
			throw new DrumException("Error synchronizing buckets with repository!", e);
		}
	}

	@Override
	protected void reset()
	{
		// can be ignored
	}
}
