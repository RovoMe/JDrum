package at.rovo.caching.drum.internal;

import at.rovo.caching.drum.Broker;
import at.rovo.caching.drum.DiskWriter;
import at.rovo.caching.drum.DrumEventDispatcher;
import at.rovo.caching.drum.DrumException;
import at.rovo.caching.drum.DrumOperation;
import at.rovo.caching.drum.Merger;
import at.rovo.caching.drum.event.DiskWriterEvent;
import at.rovo.caching.drum.event.DiskWriterState;
import at.rovo.caching.drum.event.DiskWriterStateUpdate;
import at.rovo.caching.drum.util.DiskFileHandle;
import at.rovo.common.annotations.GuardedBy;
import at.rovo.common.annotations.ThreadSafe;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Queue;
import java.util.concurrent.Semaphore;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * <em>DiskBucketWriter</em> is a consumer in the producer-consumer pattern and it takes data stored from the in-memory
 * buffer and writes it to the attached disk bucket file.
 * <p>
 * According to the paper 'IRLbot: Scaling to 6 Billion Pages and Beyond' the data is separated into key/value and
 * auxiliary data files which are stored in the cache/'drumName' directory located inside the application directory
 * where <code>drumName</code> is the name of the current Drum instance.
 * <p>
 * The current implementation uses the blocking method <code>takeAll()</code> from {@link at.rovo.caching.drum.Broker}
 * to collect the items to write into the disk file.
 *
 * @param <V>
 *         The type of the value object
 * @param <A>
 *         The type of the auxiliary data object
 *
 * @author Roman Vottner
 */
@ThreadSafe
public class DiskBucketWriter<V extends Serializable, A extends Serializable> implements DiskWriter
{
    /** The logger of this class **/
    private final static Logger LOG = LogManager.getLogger(DiskBucketWriter.class);

    // immutable fields
    /** The name of the DRUM instance **/
    private final String drumName ;
    /** The bucket ID this instance reads from and writes to **/
    private final int bucketId;
    /** The size of a bucket before a merge action is invoked **/
    private final int bucketByteSize;
    /** The broker we get data to write from **/
    private final Broker<InMemoryEntry<V, A>, V> broker;
    /** The merger who takes care of merging disk files with the backing data store. It needs to be informed if it should
     * merge, which happens if the bytes written to the disk file exceeds certain limits **/
    private final Merger merger;
    /** The object responsible for updating listeners on state or statistic changes **/
    private final DrumEventDispatcher eventDispatcher;
    /** As semaphores can be used from different threads use it here as a lock for getting access to the disk bucket
     * file **/
    private final Semaphore lock = new Semaphore(1);
    /** The reference to the key/value file **/
    @GuardedBy("lock")
    private final DiskFileHandle kvFile;
    /** The reference to the auxiliary data file attached to a key **/
    @GuardedBy("lock")
    private final DiskFileHandle auxFile;

    // mutable fields
    /** The number of bytes written into the key/value file **/
    private long kvBytesWritten = 0L;
    /** The number of bytes written into the auxiliary data file **/
    private long auxBytesWritten = 0L;
    /** flag if merging is required **/
    private boolean mergeRequired = false;
    /** Indicates if the thread the runnable part is running in should stop its work **/
    private volatile boolean stopRequested = false;
    /** Used to reduce multiple WAITING_FOR_MERGE_REQUEST event updates to a single update **/
    private DiskWriterState lastState = null;

    /**
     * Creates a new instance and instantiates required fields.
     *
     * @param drumName
     *         The name of the Drum instance
     * @param bucketId
     *         The index of the bucket this writer will read data from or write to
     * @param bucketByteSize
     *         The size in bytes before a merge with the backing data store is invoked
     * @param broker
     *         The broker who administers the in memory data
     */
    public DiskBucketWriter(String drumName, int bucketId, int bucketByteSize, Broker<InMemoryEntry<V, A>, V> broker,
                            Merger merger, DrumEventDispatcher eventDispatcher) throws DrumException
    {
        this.drumName = drumName;
        this.bucketId = bucketId;
        this.bucketByteSize = bucketByteSize;
        this.broker = broker;
        this.merger = merger;
        this.eventDispatcher = eventDispatcher;

        // check if the cache sub-directory exists - if not create one
        File cacheDir = new File(System.getProperty("user.dir") + "/cache");
        if (!cacheDir.exists())
        {
            if (!cacheDir.mkdir())
            {
                throw new DrumException("No cache directory found and could not initialize one!");
            }
        }
        // check if a sub-directory inside the cache sub-directory exists that has the name of this instance - if not
        // create it
        File file = new File(System.getProperty("user.dir") + "/cache/" + this.drumName);
        if (!file.exists())
        {
            if (!file.mkdir())
            {
                throw new DrumException("No cache data dir found and could not initialize one!");
            }
        }

        try
        {
            String fileName = "bucket" + bucketId;
            this.kvFile = new DiskFileHandle(fileName + ".kv",
                                             new RandomAccessFile(new File(cacheDir, "/" + fileName + ".kv"), "rw"));
            this.auxFile = new DiskFileHandle(fileName + ".aux",
                                              new RandomAccessFile(new File(cacheDir, "/" + fileName + ".aux"), "rw"));
        }
        catch (Exception e)
        {
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
                if (this.lastState != DiskWriterState.WAITING_FOR_DATA)
                {
                    LOG.debug("[{}] - [{}] - waiting for data", this.drumName, this.bucketId);
                    this.lastState = updateState(DiskWriterState.WAITING_FOR_DATA);
                }

                // takeAll() waits on the broker instance to retrieve data or throw an interrupted exception
                Queue<InMemoryEntry<V, A>> elementsToPersist = this.broker.takeAll();

                // skip further processing if no data was available
                if (elementsToPersist == null || elementsToPersist.size() == 0)
                {
                    continue;
                }

                this.lastState = updateState(DiskWriterState.DATA_RECEIVED);

                LOG.debug("[{}] - [{}] - received {} data elements",
                          this.drumName, this.bucketId, elementsToPersist.size());
                // write data to the disk bucket files
                this.feedBucket(elementsToPersist);
                // check if a merged is required due to exceeding the bucket byte size; if so signal a merge request!
                if (this.mergeRequired)
                {
                    this.merger.requestMerge();
                }

                this.mergeRequired = false;
            }
            catch (Exception e)
            {
                LOG.error("[{}] - [{}] - caught exception: {}", this.drumName, this.bucketId, e.getLocalizedMessage());
                LOG.catching(Level.ERROR, e);
                updateState(DiskWriterState.FINISHED_WITH_ERROR);
                Thread.currentThread().interrupt();
            }
        }
        // push the latest data which has not yet been written to the data store to be written
        if (this.kvBytesWritten > 0)
        {
            this.merger.requestMerge();
        }
        updateState(DiskWriterState.FINISHED);
        LOG.trace("[{}] - [{}] - stopped processing!", this.drumName, this.bucketId);
    }

    /**
     * Generates an {@link DiskWriterStateUpdate} event for the provided state.
     *
     * @param newState
     *         The new state this instance is in
     *
     * @return The new state of this instance
     */
    private DiskWriterState updateState(DiskWriterState newState) {
        this.eventDispatcher.update(new DiskWriterStateUpdate(this.drumName, this.bucketId, newState));
        return newState;
    }

    /**
     * Generates an {@link DiskWriterEvent} for the given <em>byteLengthKV</em> and <em>byteLengthAux</em> values.
     *
     * @param byteLengthKV
     *         The length of the key-value pair bytes
     * @param byteLengthAux
     *         The length of the auxiliary data bytes
     */
    private void updateState(long byteLengthKV, long byteLengthAux) {
        this.eventDispatcher.update(new DiskWriterEvent(this.drumName, this.bucketId, byteLengthKV, byteLengthAux));
    }

    /**
     * Closes a previously opened {@link RandomAccessFile} and frees resources held by the application.
     *
     * @param bucketFile
     *         The previously opened bucket file which needs to be closed
     */
    private void closeFile(RandomAccessFile bucketFile) throws DrumException
    {
        try
        {
            bucketFile.close();
        }
        catch (Exception e)
        {
            throw new DrumException("Exception closing disk bucket!");
        }
        finally
        {
            LOG.debug("[{}] - [{}] - Closing file {}", this.drumName, this.bucketId, bucketFile);
        }
    }

    /**
     * Feeds the key/value and auxiliary bucket files with the data stored in memory buffers.
     *
     * @param inMemoryData
     *         The buffer which contains the data to persist to disk
     */
    private void feedBucket(Queue<InMemoryEntry<V, A>> inMemoryData) throws DrumException
    {
        boolean lockAquired = false;
        try
        {
            if (inMemoryData.isEmpty())
            {
                // no data to write
                updateState(DiskWriterState.EMPTY);
                return;
            }

            updateState(DiskWriterState.WAITING_FOR_LOCK);
            // do not interrupt the file access as otherwise data might get lost!
            this.lock.acquireUninterruptibly();
            LOG.trace("[{}] - [{}] - Acquired lock of disk bucket file", this.drumName, this.bucketId);
            lockAquired = true;
            updateState(DiskWriterState.WRITING);
            final RandomAccessFile kvFile = this.kvFile.getFile();
            final RandomAccessFile auxFile = this.auxFile.getFile();

            long kvStart = kvFile.getFilePointer();
            long auxStart = auxFile.getFilePointer();

            for (InMemoryEntry<V, A> data : inMemoryData)
            {
                LOG.info("[{}] - [{}] - feeding bucket with: {}; value: {}",
                         this.drumName, this.bucketId, data.getKey(), data.getValue());
                long kvStartPos = kvFile.getFilePointer();
                long auxStartPos = auxFile.getFilePointer();

                // Write the following sequentially for the key/value bucket file:
                // - operation; (1 byte)
                // - key; (8 byte)
                // - value length; (4 byte)
                // - value. (variable byte)

                // write the operation

                DrumOperation op = data.getOperation();
                char c = op.getTokenForOperation();
                kvFile.write(c);

                // write the key
                kvFile.writeLong(data.getKey());

                // write the value
                byte[] byteValue = data.getValueAsBytes();
                if (byteValue != null)
                {
                    kvFile.writeInt(byteValue.length);
                    kvFile.write(byteValue);
                }
                else
                {
                    kvFile.writeInt(0);
                }

                long kvEndPos = kvFile.getFilePointer();
                if (byteValue != null)
                {
                    LOG.info("[{}] - [{}] - wrote to kvBucket file - operation: '{}' key: '{}', value.length: '{}' " +
                             "byteValue: '{}' and value: '{}' - bytes written " + "in total: {}",
                             this.drumName, this.bucketId, c, data.getKey(), byteValue.length,
                             Arrays.toString(byteValue), data.getValue(), (kvEndPos - kvStartPos));
                }
                else
                {
                    LOG.info("[{}] - [{}] - wrote to kvBucket file - operation: '{}' key: '{}', value.length: '0' " +
                             "byteValue: 'null' and value: '{}' - bytes written " + "in total: {}",
                             this.drumName, this.bucketId, c, data.getKey(), data.getValue(), (kvEndPos - kvStartPos));
                }

                // Write the following sequentially for the auxiliary data bucket file:
                // - aux length; (4 byte)
                // - aux. (variable byte)

                byte[] byteAux = data.getAuxiliaryAsBytes();
                if (byteAux != null)
                {
                    auxFile.writeInt(byteAux.length);
                    auxFile.write(byteAux);
                }
                else
                {
                    auxFile.writeInt(0);
                }

                long auxEndPos = auxFile.getFilePointer();
                if (byteAux != null)
                {
                    LOG.info("[{}] - [{}] - wrote to auxBucket file - aux.length: '{}' byteAux: '{}' and aux: '{}' - "
                             + "bytes written in total: {}",
                             this.drumName, this.bucketId, byteAux.length, Arrays.toString(byteAux),
                             data.getAuxiliary(), (auxEndPos - auxStartPos));
                }
                else
                {
                    LOG.info("[{}] - [{}] - wrote to auxBucket file - aux.length: '0' byteAux: 'null' and aux: '{}' - "
                             + "bytes written in total: {}",
                             this.drumName, this.bucketId, data.getAuxiliary(), (auxEndPos - auxStartPos));
                }
            }

            this.kvBytesWritten += (kvFile.getFilePointer() - kvStart);
            this.auxBytesWritten += (auxFile.getFilePointer() - auxStart);

            updateState(this.kvBytesWritten, this.auxBytesWritten);

            // is it merge time?
            if (this.kvBytesWritten > this.bucketByteSize || this.auxBytesWritten > this.bucketByteSize)
            {
                LOG.info("[{}] - [{}] - requesting merge", this.drumName, this.bucketId);
                this.mergeRequired = true;
            }
        }
        catch (Exception e)
        {
            throw new DrumException("Error feeding bucket " + this.drumName + "-" + this.bucketId + "!", e);
        }
        finally
        {
            if (lockAquired)
            {
                LOG.trace("[{}] - [{}] - Releasing lock of disk bucket file", this.drumName, this.bucketId);
                this.lock.release();
            }
        }
    }

    @Override
    public Semaphore accessDiskFile()
    {
        return this.lock;
    }

    public DiskFileHandle getKVFile()
    {
        return this.kvFile;
    }

    public DiskFileHandle getAuxFile()
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
            this.kvFile.getFile().seek(0);
            this.auxFile.getFile().seek(0);
        }
        catch (IOException e)
        {
            LOG.error("Error while resetting the disk bucket pointers of " + this.drumName + "-" + this.bucketId + "!",
                      e);
        }

        updateState(this.kvBytesWritten, this.auxBytesWritten);
        updateState(DiskWriterState.EMPTY);
    }

    @Override
    public void stop()
    {
        this.stopRequested = true;
        LOG.trace("[{}] - [{}] - stop requested!", this.drumName, this.bucketId);
    }

    @Override
    public void close()
    {
        try
        {
            this.closeFile(this.kvFile.getFile());
            this.closeFile(this.auxFile.getFile());
        }
        catch (DrumException e)
        {
            LOG.error("Error while closing disk bucket writer " + this.drumName + "-" + this.bucketId + "!", e);
        }
    }
}
