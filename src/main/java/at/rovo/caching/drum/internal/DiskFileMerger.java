package at.rovo.caching.drum.internal;

import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import at.rovo.caching.drum.DrumException;
import at.rovo.caching.drum.DrumOperation;
import at.rovo.caching.drum.DrumResult;
import at.rovo.caching.drum.IDiskWriter;
import at.rovo.caching.drum.IDispatcher;
import at.rovo.caching.drum.IMerger;
import at.rovo.caching.drum.NotAppendableException;
import at.rovo.caching.drum.data.ByteSerializer;
import at.rovo.caching.drum.event.DrumEventDispatcher;
import at.rovo.caching.drum.event.MergerState;
import at.rovo.caching.drum.event.MergerStateUpdate;
import at.rovo.caching.drum.event.StorageEvent;
import at.rovo.caching.drum.util.KeyComparator;

/**
 * <p>
 * <em>DiskFileMerger</em> merges all disk files with the backing data store and
 * classifies stored data into {@link DrumResult#DUPLICATE_KEY} and
 * {@link DrumResult#UNIQUE_KEY}.
 * </p>
 * <p>
 * It therefore applies the 4 steps presented by Lee, Wang, Leonard and Loguinov
 * in there paper 'IRLbot: Scaling to 6 Billion Pages and Beyond' to achieve
 * merging in a single one pass through disk strategy.
 * </p>
 * <p>
 * The execution of the merge happens, by default, in the {@link Thread} this
 * instance was started with. The execution was requested by {@link #doMerge()}.
 * On calling {@link #forceMerge()} the execution of the merge is done in the
 * callers thread context.
 * </p>
 * 
 * @author Roman Vottner
 */
public abstract class DiskFileMerger<V extends ByteSerializer<V>, A extends ByteSerializer<A>>
		implements IMerger<V, A>
{
	/** The logger of this class **/
	private final static Logger logger = LogManager
			.getLogger(DiskFileMerger.class);
	/**
	 * The object responsible for updating listeners on state or statistic
	 * changes
	 **/
	protected DrumEventDispatcher eventDispatcher = null;
	/** The name of the DRUM instance this file merger is linked to **/
	protected String drumName = null;
	/** Dispatcher used to send results to for further processing **/
	protected IDispatcher<V, A> dispatcher = null;
	/**
	 * A reference to the disk writers to synchronize access to the disk files
	 * both objects need access to
	 **/
	private List<IDiskWriter<V, A>> diskWriters = null;
	/** Used during merge with disk **/
	private List<InMemoryData<V, A>> sortedMergeBuffer = null;
	/** Original positions of elements in buckets **/
	private List<Integer> unsortingHelper = null;
	/** Indicates which disk writer needs a merge **/
	private List<Boolean> mergeList = null;
	/**
	 * The class object of the value type. Necessary to safely cast the generic
	 * type to a concrete type
	 **/
	protected Class<V> valueClass = null;
	/**
	 * The class object of the auxiliary type. Necessary to safely cast the
	 * generic type to a concrete type
	 **/
	protected Class<A> auxClass = null;
	/**
	 * Indicates if the thread the runnable part is running in should stop its
	 * work
	 **/
	private volatile boolean stopRequested = false;
	/**
	 * Used to reduce multiple WAITING_ON_MERGE_REQUEST event updates to a
	 * single update
	 **/
	private MergerState lastState = null;
	/** The number of unique entries stored in the data store **/
	protected long numUniqueEntries = 0L;

	/**
	 * <p>
	 * Creates a new instance.
	 * </p>
	 */
	public DiskFileMerger(String drumName, int numBuckets,
			IDispatcher<V, A> dispatcher, Class<V> valueClass,
			Class<A> auxClass, DrumEventDispatcher eventDispatcher)
	{
		this.drumName = drumName;
		this.eventDispatcher = eventDispatcher;
		this.diskWriters = new ArrayList<IDiskWriter<V, A>>();
		this.sortedMergeBuffer = new ArrayList<InMemoryData<V, A>>();

		this.mergeList = new ArrayList<>(numBuckets);
		for (int i = 0; i < numBuckets; i++)
		{
			this.mergeList.add(Boolean.FALSE);
		}

		this.dispatcher = dispatcher;
		this.valueClass = valueClass;
		this.auxClass = auxClass;
	}

	@Override
	public void addDiskFileWriter(IDiskWriter<V, A> writer)
	{
		this.diskWriters.add(writer);
	}

	/**
	 * <p>
	 * Returns the number of unique entries stored into the data store.
	 * </p>
	 */
	@Override
	public long getNumberUniqueEntriesStored()
	{
		return this.numUniqueEntries;
	}

	@Override
	public void run()
	{
		while (!this.stopRequested)
		{
			try
			{
				// check if the state was set in the previous run - this should
				// reduce the number of state updates to a single one for a
				// WAITING_ON_MERGE_REQUEST
				if (this.lastState == null)
				{
					this.lastState = MergerState.WAITING_ON_MERGE_REQUEST;
					this.eventDispatcher
							.update(new MergerStateUpdate(this.drumName,
									MergerState.WAITING_ON_MERGE_REQUEST));
				}

				if (this.mergeList.contains(Boolean.TRUE))
				{
					this.lastState = null;
					this.eventDispatcher.update(new MergerStateUpdate(
							this.drumName, MergerState.MERGE_REQUESTED));

					if (logger.isDebugEnabled())
						logger.debug("[" + this.drumName
								+ "] - Notify received! Merging data");

					// we have been notified by a writer that its disk file
					// reached a certain limit - so merge all the data in a
					// single one-pass-through strategy
					this.merge(false);
				}
				else
				{
					try
					{
						Thread.sleep(10);
					}
					catch (InterruptedException e)
					{
						e.printStackTrace();
					}
				}
			}
			catch (Exception e)
			{
				if (logger.isDebugEnabled())
					logger.debug("[" + this.drumName + "] - got interrupted");
				this.eventDispatcher.update(new MergerStateUpdate(
						this.drumName, MergerState.FINISHED_WITH_ERRORS));
				break;
			}
		}
		// a last run-through to catch data that was written between the last
		// merge and the stop-request
		if (this.mergeList.contains(Boolean.TRUE))
		{
			this.merge(false);
		}

		try
		{
			this.close();
		}
		catch (DrumException e)
		{
			if (logger.isErrorEnabled())
				logger.error(e.getMessage(), e);
			e.printStackTrace();
		}
		this.eventDispatcher.update(new MergerStateUpdate(this.drumName,
				MergerState.FINISHED));
	}

	@Override
	public void stop()
	{
		this.stopRequested = true;
	}

	/**
	 * <p>
	 * Closes resources held by the instance.
	 * </p>
	 */
	protected abstract void close() throws DrumException;

	@Override
	public void doMerge()
	{
		String threadName = Thread.currentThread().getName();
		String sBucketId = threadName.substring(threadName.indexOf("-Writer-")
				+ "-Writer-".length());
		int bucketId = Integer.parseInt(sBucketId);
		this.mergeList.set(bucketId, Boolean.TRUE);
	}

	@Override
	public void forceMerge()
	{
		// merge is synchronized which should prevent multiple threads to invoke
		// the same method if a thread is already executing it
		this.merge(true);
	}

	/**
	 * <p>
	 * Extracts the data from the disk files which need to be merged into the
	 * backing data store.
	 * </p>
	 * <p>
	 * This method applies the 4 steps presented in the paper 'IRLbot: Scaling
	 * to 6 Billion Pages and Beyond' to utilize a one pass through disk file
	 * strategy.
	 * </p>
	 */
	public synchronized void merge(boolean forced)
	{
		if (logger.isDebugEnabled())
			logger.debug("[" + this.drumName
					+ "] - merging disk files (forced = " + forced + ")");
		for (IDiskWriter<V, A> writer : this.diskWriters)
		{
			// try to lock the disk file so that the current disk
			// writer can't write to the disk file while we are
			// reading from it
			// if the writer is currently writing to disk let him
			// first finish his task
			try
			{
				this.eventDispatcher.update(new MergerStateUpdate(
						this.drumName, MergerState.WAITING_ON_LOCK, writer
								.getBucketId()));
				if (logger.isDebugEnabled())
					logger.debug("[" + this.drumName + "] - ["
							+ writer.getBucketId()
							+ "] - available permits for diskWriter: "
							+ writer.accessDiskFile().availablePermits());
				writer.accessDiskFile().acquire();

				this.eventDispatcher.update(new MergerStateUpdate(
						this.drumName, MergerState.MERGING, writer
								.getBucketId()));
				if (logger.isDebugEnabled())
					logger.debug("[" + this.drumName + "] - ["
							+ writer.getBucketId()
							+ "] - got lock of disk writer");
				// writer has finished his work and we have the lock
				// to the disk file

				// Here the 4 steps of merging the bucket files into the backing
				// data store are managed:
				//
				// 1) read key/value disk file into the bucket buffer and sort
				// it
				if (this.readDataFromDiskFile(writer))
				{
					this.sortMergeBuffer();
					// 2) backing data store is sequentially read in chunks of
					// delta
					// bytes and compared with the keys in the sorted bucket
					// buffer
					// ... combined into 3)
					// 3) those key/value pairs that require an update are
					// merged with
					// the contents of the disk cache and written to the data
					// store
					this.compareDataWithDataStore(this.sortedMergeBuffer);
					// 4) after all unique keys are found the original order of
					// the
					// bucket buffer is restored and the auxiliary data file is
					// read in chunks of delta bytes to process auxiliary data
					// of
					// unique keys
					this.unsortMergeBuffer();
					this.readAuxBucketForDispatching(writer);
					this.dispatch();

					if (logger.isDebugEnabled())
						logger.debug("[" + this.drumName + "] - ["
								+ writer.getBucketId()
								+ "] - resetting disk file "
								+ writer.getBucketId());
					// reset the cursors of the files back to the start
					writer.reset();

					this.eventDispatcher.update(new StorageEvent(this.drumName,
							this.numUniqueEntries));
				}
				else if (logger.isDebugEnabled())
					logger.debug("[" + this.drumName + "] - ["
							+ writer.getBucketId() + "] - nothing to merge");
			}
			catch (Exception e)
			{
				if (logger.isErrorEnabled())
					logger.error(
							"["
									+ this.drumName
									+ "] Error merging disk bucket files with data storage! Reason: "
									+ e.getLocalizedMessage(), e);
				e.printStackTrace();
			}
			finally
			{
				if (this.unsortingHelper != null)
					this.unsortingHelper.clear();
				if (this.sortedMergeBuffer != null)
					this.sortedMergeBuffer.clear();

				this.mergeList.set(writer.getBucketId(), Boolean.FALSE);
				writer.accessDiskFile().release();
			}
		}

		this.reset();
		System.gc();
	}

	/**
	 * <p>
	 * Reads key/value pairs from a disk file and stores them in <code>
	 * sortedMergeBuffer</code> as {@link InMemoryData} objects.
	 * </p>
	 * 
	 * @param writer
	 *            The already locked writer instance we access its disk file
	 * @throws InterruptedException
	 * @throws DrumException
	 */
	private boolean readDataFromDiskFile(IDiskWriter<V, A> writer)
			throws InterruptedException, DrumException
	{
		if (writer.getKVFileBytesWritten() == 0)
			return false;

		boolean success = false;
		if (logger.isDebugEnabled())
			logger.debug("[" + this.drumName
					+ "] - Reading data from disk file");
		try
		{
			RandomAccessFile kvFile = writer.getKVFile();
			if (kvFile == null)
				return false;
			kvFile.seek(0);

			long writtenBytes = writer.getKVFileBytesWritten();
			if (logger.isDebugEnabled())
				logger.debug("[" + this.drumName + "] - ["
						+ writer.getBucketId() + "] - reading " + writtenBytes
						+ " bytes from bucket file");

			while (kvFile.getFilePointer() < writtenBytes)
			{
				// create a new in memory data object which will hold the data
				// of the key/value file
				InMemoryData<V, A> data = new InMemoryData<>();

				// add the data element to the list we will sort later on
				this.sortedMergeBuffer.add(data);
				// keep track of the original position
				data.setPosition(this.sortedMergeBuffer.size() - 1);

				// Set the operation of the query the key was sent with
				DrumOperation op = null;
				char c = (char) kvFile.readByte();
				if (c == 'c')
					op = DrumOperation.CHECK;
				else if (c == 'u')
					op = DrumOperation.UPDATE;
				else if (c == 'b')
					op = DrumOperation.CHECK_UPDATE;
				else if (c == 'a')
					op = DrumOperation.APPEND_UPDATE;
				data.setOperation(op);

				// Retrieve the key from the file - as it is 64-bit long,
				// reading
				// long should work
				long key = kvFile.readLong();
				data.setKey(key);

				int valueSize = kvFile.readInt();
				byte[] byteValue = null;
				if (valueSize > 0)
				{
					byteValue = new byte[valueSize];
					kvFile.read(byteValue);
					// V value = DrumUtil.deserialize(byteValue,
					// this.valueClass);
					V value = this.valueClass.newInstance()
							.readBytes(byteValue);
					data.setValue(value);
				}
				if (logger.isDebugEnabled())
					logger.debug("[" + this.drumName + "] - ["
							+ writer.getBucketId()
							+ "] - read from bucket file - operation: '" + op
							+ "', key: '" + key + "', value.length: '"
							+ valueSize + "' valueBytes: '"
							+ Arrays.toString(byteValue) + "'");
			}
			this.unsortingHelper = new ArrayList<Integer>(
					this.sortedMergeBuffer.size());

			success = true;
		}
		catch (Exception e)
		{
			if (logger.isErrorEnabled())
				logger.error("[" + this.drumName
						+ "] - Error during reading key/values from bucket file! Reason: "
						+ e.getLocalizedMessage(), e);
			e.printStackTrace();
			throw new DrumException(
					"Error during reading key/values from bucket file! Reason: "
							+ e.getLocalizedMessage(), e);
		}

		return success;
	}

	/**
	 * <p>
	 * Sorts the merge buffer based on the key-value.
	 * </p>
	 */
	private void sortMergeBuffer()
	{
		if (logger.isDebugEnabled())
			logger.debug("[" + this.drumName + "] - sorting merge buffer");
		Collections.sort(this.sortedMergeBuffer,
				new KeyComparator<InMemoryData<V, A>>());
	}

	/**
	 * <p>
	 * Checks the keys of data elements stored in the bucket buffer with keys
	 * already stored in the backing data store and merges them into the data
	 * store if they need an update.
	 * </p>
	 * <p>
	 * If the key is already present in the data store the result field of the
	 * data object will be set to {@link DrumResult#DUPLICATE_KEY}, else to
	 * {@link DrumResult#UNIQUE_KEY}.
	 * </p>
	 * 
	 * @param data
	 *            The list of data to check against the data in the data store
	 * @return A list of {@link InMemoryData} objects that needs to be updated
	 *         in the data store
	 */
	protected abstract void compareDataWithDataStore(
			List<? extends InMemoryData<V, A>> data) throws DrumException,
			NotAppendableException;

	/**
	 * <p>
	 * Sets the cursor of the data store back to the start.
	 * </p>
	 */
	protected abstract void reset();

	/**
	 * <p>
	 * Reverts the origin order of the merge buffer. {@link #unsortingHelper}
	 * afterwards contains the origin position of the merge buffer, while the
	 * {@link #sortedMergeBuffer} is not touched!
	 * </p>
	 */
	private void unsortMergeBuffer()
	{
		if (logger.isDebugEnabled())
			logger.debug("[" + this.drumName + "] - unsorting merge buffer");
		// When elements were read into the merge buffer their original
		// positions
		// were stored. Now I use those positions as keys to "sort" them back in
		// linear time. Traversing unsortingHelper gives the indexes into
		// sortedMergeBuffer considering the original order.
		int total = this.sortedMergeBuffer.size();
		for (int i = 0; i < total; ++i)
			this.unsortingHelper.add(0);
		for (int i = 0; i < total; ++i)
		{
			InMemoryData<V, A> data = this.sortedMergeBuffer.get(i);
			int original = data.getPosition();
			this.unsortingHelper.set(original, i);
		}
	}

	/**
	 * <p>
	 * Reads a certain bucket containing auxiliary data from disk into memory.
	 * The data read is afterwards stored inside the {@link #unsortedAuxBuffer}
	 * as an {@link Auxiliary} object
	 * </p>
	 * 
	 * @param writer
	 *            The already locked writer instance we access its disk file
	 * @throws DrumException
	 */
	private void readAuxBucketForDispatching(IDiskWriter<V, A> writer)
			throws DrumException
	{
		// get the number of bytes written since the last merge
		long auxWritten = writer.getAuxFileBytesWritte();
		if (auxWritten == 0)
			return;
		// open the bucket file for auxiliary data with a certain ID
		try
		{
			if (logger.isDebugEnabled())
				logger.debug("[" + this.drumName + "] - [" + writer.getBucketId()
						+ "] - reading auxiliary bucket '"
						+ writer.getAuxFileName() + "' for dispatching");
			int i = 0;
			RandomAccessFile auxFile = writer.getAuxFile();
			if (auxFile == null)
				return;
			auxFile.seek(0);

			while (auxFile.getFilePointer() < auxWritten
					&& i < this.unsortingHelper.size())
			{
				// check if there is data available to store the auxiliary
				// information to
				if (null == this.unsortingHelper.get(i))
					continue;
				int index = unsortingHelper.get(i);
				InMemoryData<V, A> data = this.sortedMergeBuffer.get(index);

				// the first value written to the cache is always the size of
				// bytes written to the file - this is a 32bit integer value in
				// java!
				int auxSize = auxFile.readInt();
				// next the bytes of the actual auxiliary data are reserved and
				// read from the file
				byte[] byteAux = new byte[auxSize];
				auxFile.read(byteAux);
				if (auxSize > 0)
				{
					// transform the byte-array into a valid Java object of type
					// A
					// A aux = DrumUtil.deserialize(byteAux, this.auxClass);
					A aux = this.auxClass.newInstance().readBytes(byteAux);
					// ... and add it to the auxiliary object created before
					data.setAuxiliary(aux);
					if (logger.isDebugEnabled())
						logger.debug("[" + this.drumName + "] - ["
								+ writer.getBucketId() + "] - read aux data: "
								+ aux);
				}
				else
				{
					if (logger.isDebugEnabled())
						logger.debug("[" + this.drumName  + "] - [" + writer.getBucketId()
								+ "] - no data to read from auxiliary data bucket file");
					data.setAuxiliary(null);
				}

				i++; // index variable for the unsorting helper
			}

		}
		catch (Exception e)
		{
			if (logger.isErrorEnabled())
				logger.error("[" + this.drumName + "] - ["
						+ writer.getBucketId()
						+ "] - Could not read auxiliary bucket file! Reason: "
						+ e.getLocalizedMessage(), e);
			e.printStackTrace();
			throw new DrumException(
					"Could not read auxiliary bucket file! Reason: "
							+ e.getLocalizedMessage(), e);
		}
	}

	/**
	 * <p>
	 * Dispatches {@link DrumResult}s to the caller.
	 * </p>
	 * 
	 * @throws DrumException
	 */
	private void dispatch() throws DrumException
	{
		if (logger.isDebugEnabled())
			logger.debug("[" + this.drumName + "] - dispatching results");
		// get the number of data to dispatch
		int total = this.sortedMergeBuffer.size();
		for (int i = 0; i < total; ++i)
		{
			// check if there is data available to dispatch
			if (null == this.unsortingHelper.get(i))
				continue;
			// if so, get the element index of the original order
			int dataIndex = this.unsortingHelper.get(i);
			// ... and use it to retrieve the actual key/value-pair from the
			// sorted list
			InMemoryData<V, A> data = this.sortedMergeBuffer.get(dataIndex);

			// now retrieving the key, the action, the result of the query and
			// the
			// auxiliary data of the key is fairly easy
			Long key = data.getKey();
			DrumOperation op = data.getOperation();
			DrumResult result = data.getResult();
			A aux = data.getAuxiliary();

			if (logger.isDebugEnabled())
				logger.debug("[" + this.drumName + "] - dispatching: op: "
						+ data.getOperation() + "; " + data.getKey()
						+ "; value: " + data.getValue() + "; aux: "
						+ data.getAuxiliary() + "; result: " + data.getResult());

			// inform the dispatcher of the outcome
			if (DrumOperation.CHECK.equals(op)
					&& DrumResult.UNIQUE_KEY.equals(result))
				this.dispatcher.uniqueKeyCheck(key, aux);
			else
			{
				V value = data.getValue();

				if (DrumOperation.CHECK.equals(op)
						&& DrumResult.DUPLICATE_KEY.equals(result))
					this.dispatcher.duplicateKeyCheck(key, value, aux);
				else if (DrumOperation.CHECK_UPDATE.equals(op)
						&& DrumResult.UNIQUE_KEY.equals(result))
					this.dispatcher.uniqueKeyUpdate(key, value, aux);
				else if (DrumOperation.CHECK_UPDATE.equals(op)
						&& DrumResult.DUPLICATE_KEY.equals(result))
					this.dispatcher.duplicateKeyUpdate(key, value, aux);
				else if (DrumOperation.UPDATE.equals(op)
						|| DrumOperation.APPEND_UPDATE.equals(op))
					this.dispatcher.update(key, value, aux);
				else
					throw new DrumException(
							"Invalid action method selected on data element: "
									+ data);
			}
		}
	}
}