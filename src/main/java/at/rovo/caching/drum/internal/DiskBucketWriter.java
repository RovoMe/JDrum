package at.rovo.caching.drum.internal;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Semaphore;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import at.rovo.caching.drum.DrumException;
import at.rovo.caching.drum.DrumOperation;
import at.rovo.caching.drum.IBroker;
import at.rovo.caching.drum.IDiskWriter;
import at.rovo.caching.drum.IMerger;
import at.rovo.caching.drum.data.ByteSerializer;
import at.rovo.caching.drum.event.DiskWriterEvent;
import at.rovo.caching.drum.event.DiskWriterState;
import at.rovo.caching.drum.event.DiskWriterStateUpdate;
import at.rovo.caching.drum.event.DrumEventDispatcher;

/**
 * <p>
 * <em>DiskBucketWriter</em> is a consumer in the producer-consumer pattern and
 * it takes data stored from the in-memory buffer and writes it to the attached
 * disk bucket file.
 * </p>
 * <p>
 * According to the paper 'IRLbot: Scaling to 6 Billion Pages and Beyond' the
 * data is separated into key/value and auxiliary data files which are stored in
 * the cache/'drumName' directory located inside the application directory where
 * <code>drumName</code> is the name of the current Drum instance.
 * </p>
 * <p>
 * The current implementation uses the blocking method <code>takeAll()</code>
 * from {@link IBroker} to collect the items to write into the disk file.
 * </p>
 * 
 * @param <V>
 *            The type of the value object
 * @param <A>
 *            The type of the auxiliary data object
 * 
 * @author Roman Vottner
 */
public class DiskBucketWriter<V extends ByteSerializer<V>, A extends ByteSerializer<A>>
		implements IDiskWriter<V, A>
{
	/** The logger of this class **/
	private final static Logger logger = LogManager.getLogger(DiskBucketWriter.class);

	/** The name of the DRUM instance **/
	private String drumName = null;
	/** The bucket ID this instance reads from and writes to **/
	private int bucketId = 0;
	/** The size of a bucket before a merge action is invoked **/
	private int bucketByteSize = 0;
	/** The broker we get data to write from **/
	private IBroker<InMemoryData<V, A>, V, A> broker = null;
	/**
	 * The merger who takes care of merging disk files with the backing data
	 * store. It needs to be informed if it should merge, which happens if the
	 * bytes written to the disk file exceeds certain limits
	 **/
	private IMerger<V, A> merger = null;
	/**
	 * The object responsible for updating listeners on state or statistic
	 * changes
	 **/
	private DrumEventDispatcher eventDispatcher = null;
	/** The name of the disk file this instance will write key/value data to **/
	private String kvFileName = null;
	/** The name of the disk file this instance will write auxiliary data to **/
	private String auxFileName = null;
	/** The reference to the key/value file **/
	private RandomAccessFile kvFile = null;
	/** The reference to the auxiliary data file attached to a key **/
	private RandomAccessFile auxFile = null;
	/** The number of bytes written into the key/value file **/
	private long kvBytesWritten = 0L;
	/** The number of bytes written into the auxiliary data file **/
	private long auxBytesWritten = 0L;
	/** flag if merging is required **/
	private boolean mergeRequired = false;
	/**
	 * As semaphores can be used from different threads use it here as a lock
	 * for getting access to the disk bucket file
	 **/
	private Semaphore lock = new Semaphore(1);
	/**
	 * Indicates if the thread the runnable part is running in should stop its
	 * work
	 **/
	private volatile boolean stopRequested = false;
	/**
	 * Used to reduce multiple WAITING_ON_MERGE_REQUEST event updates to a
	 * single update
	 **/
	private DiskWriterState lastState = null;

	/**
	 * <p>
	 * Creates a new instance and instantiates required fields.
	 * </p>
	 * 
	 * @param drumName
	 *            The name of the Drum instance
	 * @param bucketId
	 *            The index of the bucket this writer will read data from or
	 *            write to
	 * @param bucketByteSize
	 *            The size in bytes before a merge with the backing data store
	 *            is invoked
	 * @param broker
	 *            The broker who administers the in memory data
	 */
	public DiskBucketWriter(String drumName, int bucketId, int bucketByteSize,
			IBroker<InMemoryData<V, A>, V, A> broker, IMerger<V, A> merger,
			DrumEventDispatcher eventDispatcher) throws DrumException
	{
		this.drumName = drumName;
		this.bucketId = bucketId;
		this.bucketByteSize = bucketByteSize;
		this.broker = broker;
		this.merger = merger;
		this.eventDispatcher = eventDispatcher;

		this.initDiskFile();
	}

	/**
	 * <p>
	 * Creates the bucket files for key/value and auxiliary data.
	 * </p>
	 */
	private void initDiskFile() throws DrumException
	{
		String userDir = System.getProperty("user.dir");

		// check if the cache sub-directory exists - if not create one
		File cacheDir = new File(System.getProperty("user.dir") + "/cache");
		if (!cacheDir.exists())
			cacheDir.mkdir();
		// check if a sub-directory inside the cache sub-directory exists that
		// has the
		// name of this instance - if not create it
		File file = new File(System.getProperty("user.dir") + "/cache/"
				+ this.drumName);
		if (!file.exists())
			file.mkdir();

		try
		{
			this.kvFileName = userDir + "/cache/" + this.drumName + "/bucket"
					+ bucketId + ".kv";
			this.kvFile = new RandomAccessFile(this.kvFileName, "rw");

			this.auxFileName = userDir + "/cache/" + this.drumName + "/bucket"
					+ bucketId + ".aux";
			this.auxFile = new RandomAccessFile(this.auxFileName, "rw");
		}
		catch (Exception e)
		{
			logger.error("{} - Error creating bucket file!", this.drumName, e); 
			logger.catching(e);
			throw new DrumException("Error creating bucket file!", e);
		}
	}

	@Override
	public int getBucketId()
	{
		return this.bucketId;
	}

	@Override
	public void run()
	{
		while (!this.stopRequested)
		{
			try
			{
				if (this.lastState == null)
				{
					logger.debug("[{}] - [{}] - waiting for data", this.drumName, this.bucketId);

					this.lastState = DiskWriterState.WAITING_ON_DATA;
					this.eventDispatcher.update(new DiskWriterStateUpdate(
							this.drumName, this.bucketId, DiskWriterState.WAITING_ON_DATA));
				}

				// use a blocking call to retrieve the elements to persist
				// takeAll() waits on the broker instance to retrieve data
				List<InMemoryData<V, A>> elementsToPersist = this.broker.takeAll();

				// in case a flush was invoked but there aren't any data
				// available
				if (elementsToPersist == null || elementsToPersist.size() == 0)
					continue;

				this.lastState = null;
				this.eventDispatcher.update(new DiskWriterStateUpdate(
						this.drumName, this.bucketId, DiskWriterState.DATA_RECEIVED));

				logger.debug("[{}] - [{}] - received {} data elements", 
						this.drumName, this.bucketId, elementsToPersist.size());
				this.feedBucket(elementsToPersist);

				assert (this.lock.availablePermits() == 1);

				if (this.mergeRequired)
					this.merger.doMerge();

				assert (this.lock.availablePermits() == 1);

				this.mergeRequired = false;
			}
			catch (InterruptedException iE)
			{
				if (!DiskWriterState.FINISHED.equals(this.lastState))
				{
					logger.error("[{}] - [{}] - got interrupted!", 
							this.drumName, this.bucketId);
					this.lastState = DiskWriterState.FINISHED;
				}
				this.eventDispatcher
						.update(new DiskWriterStateUpdate(this.drumName,
								this.bucketId, DiskWriterState.FINISHED));
				Thread.currentThread().interrupt();
			}
			catch (Exception e)
			{
				logger.error("[{}] - [{}] - caught exception: {}", 
						this.drumName, this.bucketId, e.getLocalizedMessage());
				this.eventDispatcher.update(new DiskWriterStateUpdate(
						this.drumName, this.bucketId,
						DiskWriterState.FINISHED_WITH_ERROR));
				e.printStackTrace();
				Thread.currentThread().interrupt();
			}
		}
		// push the latest data which has not yet been written to the data store
		// to be written
		if (this.kvBytesWritten > 0)
		{
			this.merger.doMerge();
		}
		this.eventDispatcher.update(new DiskWriterStateUpdate(this.drumName,
				this.bucketId, DiskWriterState.FINISHED));
		logger.trace("[{}] - [{}] - stopped processing!", this.drumName, this.bucketId);
	}

	/**
	 * <p>
	 * Closes a previously opened {@link RandomAccessFile} and frees resources
	 * held by the application.
	 * </p>
	 * 
	 * @param bucketFile
	 *            The previously opened bucket file which needs to be closed
	 */
	private void closeFile(RandomAccessFile bucketFile) throws DrumException
	{
		try
		{
			bucketFile.close();
		}
		catch (Exception e)
		{
			logger.error("[{}] - [{}] - Exception closing disk bucket!", 
					this.drumName, this.bucketId, e); 
			logger.catching(e);
			throw new DrumException("Exception closing disk bucket!");
		}
		finally
		{
			logger.debug("[{}] - [{}] - Closing file {}", 
					this.drumName, this.bucketId, bucketFile);
		}
	}

	/**
	 * <p>
	 * Feeds the key/value and auxiliary bucket files with the data stored in
	 * memory buffers.
	 * </p>
	 * 
	 * @param inMemoryData
	 *            The buffer which contains the data to persist to disk
	 */
	private void feedBucket(List<InMemoryData<V, A>> inMemoryData) 
			throws DrumException
	{
		try
		{
			this.eventDispatcher.update(new DiskWriterStateUpdate(
					this.drumName, this.bucketId,
					DiskWriterState.WAITING_ON_LOCK));
			this.lock.acquire();
			this.eventDispatcher.update(new DiskWriterStateUpdate(
					this.drumName, this.bucketId, DiskWriterState.WRITING));

			long kvStart = this.kvFile.getFilePointer();
			long auxStart = this.auxFile.getFilePointer();

			for (InMemoryData<V, A> data : inMemoryData)
			{
				logger.info("[{}] - [{}] - feeding bucket with: {}; value: {}",  
						this.drumName, this.bucketId, data.getKey(), data.getValue());
				long kvStartPos = this.kvFile.getFilePointer();
				long auxStartPos = this.auxFile.getFilePointer();

				// Write the following sequentially for the key/value bucket
				// file:
				// - operation; (1 byte)
				// - key; (8 byte)
				// - value length; (4 byte)
				// - value. (variable byte)

				// write the operation
				char c;
				DrumOperation op = data.getOperation();
				if (op.equals(DrumOperation.CHECK))
					c = 'c';
				else if (op.equals(DrumOperation.UPDATE))
					c = 'u';
				else if (op.equals(DrumOperation.CHECK_UPDATE))
					c = 'b'; // both; CHECK_UPDATE
				else if (op.equals(DrumOperation.APPEND_UPDATE))
					c = 'a';
				else
					c = 'n'; // nothing - should not happen!
				this.kvFile.write(c);

				// write the key
				this.kvFile.writeLong(data.getKey());

				// write the value
				byte[] byteValue = data.getValueAsBytes();
				if (byteValue != null)
				{
					this.kvFile.writeInt(byteValue.length);
					this.kvFile.write(byteValue);
				}
				else
					this.kvFile.writeInt(0);

				long kvEndPos = this.kvFile.getFilePointer();
				if (byteValue != null)
				{
					logger.info("[{}] - [{}] - wrote to kvBucket file - "
							+ "operation: '{}' key: '{}', value.length: '{}' "
							+ "byteValue: '{}' and value: '{}' - bytes written "
							+ "in total: {}", this.drumName, this.bucketId, c, 
							data.getKey(), byteValue.length, 
							Arrays.toString(byteValue), data.getValue(), 
							(kvEndPos - kvStartPos));
				}
				else
				{
					logger.info("[{}] - [{}] - wrote to kvBucket file - "
							+ "operation: '{}' key: '{}', value.length: '0' "
							+ "byteValue: 'null' and value: '{}' - bytes written "
							+ "in total: {}", this.drumName, this.bucketId, c, 
							data.getKey(), data.getValue(), 
							(kvEndPos - kvStartPos));
				}

				// Write the following sequentially for the auxiliary data
				// bucket file:
				// - aux length; (4 byte)
				// - aux. (variable byte)

				byte[] byteAux = data.getAuxiliaryAsBytes();
				if (byteAux != null)
				{
					this.auxFile.writeInt(byteAux.length);
					this.auxFile.write(byteAux);
				}
				else
					this.auxFile.writeInt(0);

				long auxEndPos = this.auxFile.getFilePointer();
				if (byteAux != null)
				{
					logger.info("[{}] - [{}] - wrote to auxBucket file - "
							+ "aux.length: '{}' byteAux: '{}' and aux: '{}' - "
							+ "bytes written in total: {}", this.drumName, 
							this.bucketId, byteAux.length, 
							Arrays.toString(byteAux), data.getAuxiliary(), 
							(auxEndPos - auxStartPos));
				}
				else
				{
					logger.info("[{}] - [{}] - wrote to auxBucket file - "
							+ "aux.length: '0' byteAux: 'null' and aux: '{}' - "
							+ "bytes written in total: {}", this.drumName, 
							this.bucketId, data.getAuxiliary(), 
							(auxEndPos - auxStartPos));
				}
			}

			this.kvBytesWritten += (this.kvFile.getFilePointer() - kvStart);
			this.auxBytesWritten += (this.auxFile.getFilePointer() - auxStart);

			this.eventDispatcher.update(new DiskWriterEvent(this.drumName,
					this.bucketId, this.kvBytesWritten, this.auxBytesWritten));

			// is it merge time? If the feed was forced the merge will be done
			// by the main-thread so do not set the merge flag therefore else
			// two threads would try to merge the data which might result in
			// a deadlock
			if (this.kvBytesWritten > this.bucketByteSize 
					|| this.auxBytesWritten > this.bucketByteSize)
			{
				logger.info("[{}] - [{}] - requesting merge", 
						this.drumName, this.bucketId);
				this.mergeRequired = true;
			}
		}
		catch (Exception e)
		{
			logger.error("[{}] - [{}] - Error feeding bucket! Reason: {}", 
					this.drumName, this.bucketId, e.getLocalizedMessage(), e); 
			logger.catching(e);
			throw new DrumException("Error feeding bucket!", e);
		}
		finally
		{
			this.lock.release();
		}
	}

	@Override
	public Semaphore accessDiskFile()
	{
		return this.lock;
	}

	@Override
	public String getKVFileName()
	{
		return this.kvFileName;
	}

	@Override
	public String getAuxFileName()
	{
		return this.auxFileName;
	}

	public RandomAccessFile getKVFile()
	{
		return this.kvFile;
	}

	public RandomAccessFile getAuxFile()
	{
		return this.auxFile;
	}

	@Override
	public long getKVFileBytesWritten()
	{
		return this.kvBytesWritten;
	}

	@Override
	public long getAuxFileBytesWritte()
	{
		return this.auxBytesWritten;
	}

	@Override
	public void reset()
	{
		// set the bytes written to 0
		this.kvBytesWritten = 0L;
		this.auxBytesWritten = 0L;

		// set the file pointer to the start of the file
		try
		{
			this.kvFile.seek(0);
			this.auxFile.seek(0);
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}

		this.eventDispatcher.update(new DiskWriterEvent(this.drumName,
				this.bucketId, this.kvBytesWritten, this.auxBytesWritten));
		this.eventDispatcher.update(new DiskWriterStateUpdate(this.drumName,
				this.bucketId, DiskWriterState.EMPTY));
	}

	@Override
	public void stop()
	{
		this.stopRequested = true;
		logger.trace("[{}] - [{}] - stop requested!", this.drumName, this.bucketId);
	}

	@Override
	public void close()
	{
		try
		{
			this.closeFile(this.kvFile);
			this.closeFile(this.auxFile);
		}
		catch (DrumException e)
		{
			logger.catching(e);
		}
	}
}
