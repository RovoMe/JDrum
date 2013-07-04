package at.rovo.caching.drum.internal.backend.cacheFile;

import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import at.rovo.caching.drum.DrumException;
import at.rovo.caching.drum.DrumOperation;
import at.rovo.caching.drum.DrumResult;
import at.rovo.caching.drum.IDispatcher;
import at.rovo.caching.drum.NotAppendableException;
import at.rovo.caching.drum.data.ByteSerializer;
import at.rovo.caching.drum.event.DrumEventDispatcher;
import at.rovo.caching.drum.internal.DiskFileMerger;
import at.rovo.caching.drum.internal.InMemoryData;

/**
 * <p>
 * <em>CacheFileMerger</em> uses {@link CacheFile} to check keys for their
 * uniqueness and merges data that needs to be updated with the cache file.
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
public class CacheFileMerger<V extends ByteSerializer<V>, A extends ByteSerializer<A>>
		extends DiskFileMerger<V, A>
{
	// create a logger
	private final static Logger logger = LogManager
			.getLogger(CacheFileMerger.class);

	/** A reference to the actual cache file **/
	private CacheFile<V> cacheFile = null;

	/**
	 * <p>
	 * Creates a new instance and initializes required instance fields.
	 * </p>
	 * 
	 * @param drumName
	 *            The name of the DRUM instance this cache is used for
	 * @param dispatcher
	 *            The dispatching instance to send results to
	 * @throws DrumException
	 */
	public CacheFileMerger(String drumName, int numBuckets,
			IDispatcher<V, A> dispatcher, Class<V> valueClass,
			Class<A> auxClass, DrumEventDispatcher eventDispatcher)
			throws DrumException
	{
		super(drumName, numBuckets, dispatcher, valueClass, auxClass,
				eventDispatcher);

		this.initCacheFile();
	}

	/**
	 * <p>
	 * Initializes and creates if necessary the directories and files for the
	 * cache file.
	 * </p>
	 * <p>
	 * Note that the cache file will be placed inside a cache directory of the
	 * applications root-path in a further directory which is named after the
	 * DRUM instance it is created for. The actual cache file is named <code>
	 * cache.db</code>.
	 * </p>
	 * 
	 * @throws DrumException
	 *             If the cache file could not be created
	 */
	private void initCacheFile() throws DrumException
	{
		String appDir = System.getProperty("user.dir");
		File cacheDir = new File(appDir + "/cache");
		if (!cacheDir.exists())
			cacheDir.mkdir();
		File drumDir = new File(cacheDir + "/" + drumName);
		if (!drumDir.exists())
			drumDir.mkdir();
		File cache = new File(drumDir + "/cache.db");
		if (!cache.exists())
		{
			try
			{
				cache.createNewFile();
			}
			catch (IOException e)
			{
				throw new DrumException(
						"Error while creating data store cache.db! Reason: "
								+ e.getLocalizedMessage());
			}
		}

		this.cacheFile = new CacheFile<V>(drumDir + "/cache.db", this.drumName,
				this.valueClass);
	}

	@Override
	protected void compareDataWithDataStore(
			List<? extends InMemoryData<V, A>> data) throws DrumException,
			NotAppendableException
	{
		for (InMemoryData<V, A> element : data)
		{
			Long key = element.getKey();
			DrumOperation op = element.getOperation();

			assert ((DrumOperation.CHECK.equals(op)
					|| DrumOperation.CHECK_UPDATE.equals(op) || DrumOperation.UPDATE
						.equals(op)) || DrumOperation.APPEND_UPDATE.equals(op));

			// set the result for CHECK and CHECK_UPDATE operations for a
			// certain key
			if (DrumOperation.CHECK.equals(op)
					|| DrumOperation.CHECK_UPDATE.equals(op))
			{
				try
				{
					InMemoryData<V, ?> entry = this.cacheFile.getEntry(key);
					if (entry == null)
					{
						element.setResult(DrumResult.UNIQUE_KEY);
					}
					else
					{
						element.setResult(DrumResult.DUPLICATE_KEY);
						// in case we have a duplicate check element
						// set its value to the contained value
						// checkUpdates contain already the latest entry t the
						// time the result will be processed, as the value
						// will be written to the data store in the next section
						if (DrumOperation.CHECK.equals(element.getOperation()))
							element.setValue(entry.getValue());
					}
				}
				catch (IOException | InstantiationException
						| IllegalAccessException e)
				{
					if (logger.isErrorEnabled())
						logger.error("[" + this.drumName
								+ "] - Error retrieving data object for key: "
								+ key + " (" + element + ")! Reason: "
								+ e.getLocalizedMessage(), e);
					throw new DrumException(
							"Error retrieving data object with key: " + key
									+ "!", e);
				}
			}

			// update the value of a certain key in case of UPDATE or
			// CHECK_UPDATE operations
			// within the bucket file
			if (DrumOperation.UPDATE.equals(op)
					|| DrumOperation.CHECK_UPDATE.equals(op))
			{
				try
				{
					this.cacheFile.writeEntry(element, false);
					this.numUniqueEntries = this.cacheFile.getNumberOfEntries();
				}
				catch (IOException | InstantiationException
						| IllegalAccessException e)
				{
					if (logger.isErrorEnabled())
						logger.error("[" + this.drumName
								+ "] - Error writing data object for key: "
								+ key + " (" + element + ")! Reason: "
								+ e.getLocalizedMessage(), e);
					throw new DrumException(
							"Error writing data object with key: " + key + "!",
							e);
				}
			}
			// data object contains request to append the data to the existing
			// data in case the entry already exists - if not it is created
			else if (DrumOperation.APPEND_UPDATE.equals(op))
			{
				try
				{
					element.setValue(this.cacheFile.writeEntry(element, true)
							.getValue());
					this.numUniqueEntries = this.cacheFile.getNumberOfEntries();
				}
				catch (IOException | InstantiationException
						| IllegalAccessException e)
				{
					if (logger.isErrorEnabled())
						logger.error("[" + this.drumName
								+ "] - Error writing data object for key: "
								+ key + " (" + element + ")! Reason: "
								+ e.getLocalizedMessage(), e);
					throw new DrumException(
							"Error writing data object with key: " + key + "!",
							e);
				}
			}

			if (logger.isInfoEnabled())
				logger.info("[" + this.drumName + "] - synchronizing key: '"
						+ key + "' operation: '" + op
						+ "' with repository - result: '" + element.getResult()
						+ "'");
		}
	}

	@Override
	public void reset()
	{
		this.cacheFile.reset();
	}

	@Override
	public void close() throws DrumException
	{
		if (this.cacheFile != null)
			this.cacheFile.close();
	}

	/**
	 * <p>
	 * Returns a reference to the cache file used to store the data.
	 * </p>
	 * 
	 * @return A reference to the cache file
	 */
	public CacheFile<V> getCacheFile()
	{
		return this.cacheFile;
	}
}
