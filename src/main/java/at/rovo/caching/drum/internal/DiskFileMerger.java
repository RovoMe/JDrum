package at.rovo.caching.drum.internal;

import at.rovo.caching.drum.DiskWriter;
import at.rovo.caching.drum.Dispatcher;
import at.rovo.caching.drum.DrumEventDispatcher;
import at.rovo.caching.drum.DrumException;
import at.rovo.caching.drum.DrumOperation;
import at.rovo.caching.drum.DrumResult;
import at.rovo.caching.drum.DrumStoreEntry;
import at.rovo.caching.drum.Merger;
import at.rovo.caching.drum.NotAppendableException;
import at.rovo.caching.drum.event.MergerState;
import at.rovo.caching.drum.event.MergerStateUpdate;
import at.rovo.caching.drum.event.StorageEvent;
import at.rovo.caching.drum.util.DrumUtils;
import at.rovo.caching.drum.util.KeyComparator;
import at.rovo.common.annotations.GuardedBy;
import at.rovo.common.annotations.ThreadSafe;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * <em>DiskFileMerger</em> merges all disk files with the backing data store and classifies stored data into {@link
 * DrumResult#DUPLICATE_KEY} and {@link DrumResult#UNIQUE_KEY}.
 * <p>
 * It therefore applies the 4 steps presented by Lee, Wang, Leonard and Loguinov in there paper 'IRLbot: Scaling to 6
 * Billion Pages and Beyond' to achieve merging in a single one pass through disk strategy.
 * <p>
 * The execution of the merge happens, by default, in the {@link Thread} this instance was started with. The execution
 * was requested by {@link #requestMerge()}.
 *
 * @author Roman Vottner
 */
@ThreadSafe
public abstract class DiskFileMerger<V extends Serializable, A extends Serializable> implements Merger
{
    /** The logger of this class **/
    private final static Logger LOG = LogManager.getLogger(DiskFileMerger.class);

    // immutable members
    /** The name of the DRUM instance this file merger is linked to **/
    protected final String drumName ;
    /** The class object of the value type. Necessary to safely cast the generic type to a concrete type **/
    protected final Class<V> valueClass;
    /** The class object of the auxiliary type. Necessary to safely cast the generic type to a concrete type. As child
     * implementation should not store auxiliary data this field is kept private. **/
    private final Class<A> auxClass;
    /** Dispatcher used to send results to for further processing **/
    protected final Dispatcher<V, A> dispatcher;
    /** The object responsible for updating listeners on state or statistic changes **/
    private final DrumEventDispatcher eventDispatcher;
    /** A reference to the disk writers to synchronize access to the disk files both objects need access to **/
    @GuardedBy("DiskWriter.accessDiskFile()")
    private final List<DiskWriter> diskWriters;
    /** The merge lock is needed in order to let the merger thread wait for incoming merge signals by disk writer who
     * exceeded the file limit on a previous <code>feedBucket()</code> **/
    private final Lock mergeLock = new ReentrantLock();
    /** The lock condition who will perform the {@link Condition#await()} and {@link Condition#signal()} on the merging
     * thread **/
    private final Condition mergeRequest = mergeLock.newCondition();

    // mutable members
    /** Specifies if a merge was requested by one of the disk writers **/
    @GuardedBy("mergeLock")
    private volatile boolean isMergeRequested = false;

    /** Used during merge with disk **/
    private List<InMemoryEntry<V, A>> sortedMergeBuffer = null;
    /** Original positions of elements in buckets **/
    private List<Integer> unsortingHelper = null;

    /** Indicates if the thread the runnable part is running in should stop its work **/
    private volatile boolean stopRequested = false;
    /** Used to reduce multiple WAITING_FOR_MERGE_REQUEST event updates to a single update **/
    private MergerState lastState = null;
    /** The number of unique entries stored in the data store **/
    protected long numUniqueEntries = 0L;
    /** A reference of the thread which executes the merging logic. This reference is used in order to interrupt a
     * blocking thread and proceed with the execution **/
    private Thread mergerThread = null;

    /**
     * Creates a new instance.
     */
    public DiskFileMerger(String drumName, int numBuckets, Dispatcher<V, A> dispatcher, Class<V> valueClass,
                          Class<A> auxClass, DrumEventDispatcher eventDispatcher)
    {
        this.drumName = drumName;
        this.eventDispatcher = eventDispatcher;
        this.diskWriters = new ArrayList<>(numBuckets);
        this.sortedMergeBuffer = new ArrayList<>();

        this.dispatcher = dispatcher;
        this.valueClass = valueClass;
        this.auxClass = auxClass;
    }

    @Override
    public void addDiskFileWriter(DiskWriter writer)
    {
        this.diskWriters.add(writer);
    }

    @Override
    public long getNumberUniqueEntriesStored()
    {
        return this.numUniqueEntries;
    }

    @Override
    public void run()
    {
        this.mergerThread = Thread.currentThread();
        while (!this.stopRequested)
        {
            // check if the state was set in the previous run - this should reduce the number of state updates to a
            // single one for a WAITING_FOR_MERGE_REQUEST
            if (this.lastState != MergerState.WAITING_FOR_MERGE_REQUEST)
            {
                this.lastState = updateState(MergerState.WAITING_FOR_MERGE_REQUEST);
            }

            boolean lockAcquired = false;
            try
            {
                this.mergeLock.lockInterruptibly();
                LOG.trace("[{}] - Acquired lock of disk file merger", this.drumName);
                lockAcquired = true;
                // check if a merge was requested
                while (!this.isMergeRequested)
                {
                    // block till we get notified
                    this.mergeRequest.await();
                }
                this.isMergeRequested = false;
            }
            catch (InterruptedException iEx)
            {
                LOG.debug("[{}] - Merger was interrupted", this.drumName);
                break;
            }
            finally
            {
                if (lockAcquired)
                {
                    LOG.trace("[{}] - Releasing lock of disk file merger", this.drumName);
                    this.mergeLock.unlock();
                }
            }

            LOG.debug("[{}] - Notify received! Merging data", this.drumName);
            this.lastState = updateState(MergerState.MERGE_REQUESTED);
            // we have been notified by a writer that its disk file reached a certain limit - so merge all the data
            // in a single one-pass-through strategy
            this.merge();
        }
        // a last run-through to catch data that was written between the last merge and the stop-request
        this.merge();

        try
        {
            this.close();
        }
        catch (Exception e)
        {
            LOG.error("[" + this.drumName + "] Caught exception while closing the writer ", e);
            updateState(MergerState.FINISHED_WITH_ERRORS);
        }
        updateState(MergerState.FINISHED);
        LOG.trace("[{}] - stopped processing!", this.drumName);
    }

    /**
     * Generates a new event for the provided {@link MergerState event}.
     *
     * @param newState
     *         The new {@link MergerState state} to set
     *
     * @return The new state
     */
    private MergerState updateState(MergerState newState) {
        this.eventDispatcher.update(new MergerStateUpdate(this.drumName, newState));
        return newState;
    }

    /**
     * Generates a new event for the specified bucket ID.
     *
     * @param bucketId
     *         The bucket ID to generate the event for
     * @param newState
     *         The new {@link MergerState state} to set
     */
    private void updateStateForBucket(int bucketId, MergerState newState) {
        this.eventDispatcher.update(new MergerStateUpdate(this.drumName, newState, bucketId));
    }

    /**
     * Generates a new {@link StorageEvent} with the given number of unique entries.
     *
     * @param numUniqueEntries
     *         The number of unique entries
     */
    private void updateState(long numUniqueEntries) {
        this.eventDispatcher.update(new StorageEvent(this.drumName, numUniqueEntries));
    }

    @Override
    public void stop()
    {
        LOG.trace("stop requested!");
        this.stopRequested = true;
        if (this.mergerThread != null)
        {
            this.mergerThread.interrupt();
        }
    }

    /**
     * Closes resources held by the instance.
     */
    @Override
    public abstract void close() throws Exception;

    @Override
    public void requestMerge()
    {
        String threadName = Thread.currentThread().getName();
        String sBucketId = threadName.substring(threadName.indexOf("-Writer-") + "-Writer-".length());
        int bucketId = Integer.parseInt(sBucketId);
        LOG.debug("[{}] - Disk bucket {} requested merge", this.drumName, bucketId);

        this.signalMerge();
    }

    /**
     * Signals the blocked merger thread that a merge should be performed.
     */
    private void signalMerge()
    {
        this.mergeLock.lock();
        try
        {
            this.isMergeRequested = true;
            this.mergeRequest.signal();
        }
        finally
        {
            this.mergeLock.unlock();
        }
    }

    /**
     * Extracts the data from the disk files which need to be merged into the backing data store.
     * <p>
     * This method applies the 4 steps presented in the paper 'IRLbot: Scaling to 6 Billion Pages and Beyond' to utilize
     * a one pass through disk file strategy.
     */
    public void merge()
    {
        LOG.debug("[{}] - merging disk files", this.drumName);
        for (DiskWriter writer : this.diskWriters)
        {
            boolean lockAcquired = false;
            // try to lock the disk file so that the current disk writer can't write to the disk file while we are
            // reading from it. If the writer is currently writing to disk let him finish his task first
            try
            {
                updateStateForBucket(writer.getBucketId(), MergerState.WAITING_FOR_LOCK);
                LOG.debug("[{}] - [{}] - available permits for diskWriter: {}",
                          this.drumName, writer.getBucketId(), writer.accessDiskFile().availablePermits());
                // do not interrupt the file access as otherwise data might get lost!
                writer.accessDiskFile().acquireUninterruptibly();
                lockAcquired = true;
                updateStateForBucket(writer.getBucketId(), MergerState.MERGING);
                LOG.debug("[{}] - [{}] - Acquired lock of disk bucket file", this.drumName, writer.getBucketId());
                // writer has finished his work and we have the lock to the disk file

                // Here the 4 steps of merging the bucket files into the backing data store are managed:
                //
                // 1) read key/value disk file into the bucket buffer and sort it
                if (this.readDataFromDiskFile(writer))
                {
                    this.sortMergeBuffer();
                    // 2) backing data store is sequentially read in chunks of delta bytes and compared with the keys in
                    //    the sorted bucket buffer
                    // ... combined into 3)
                    // 3) those key/value pairs that require an update are merged with the contents of the disk cache
                    //    and written to the data store
                    this.compareDataWithDataStore(this.sortedMergeBuffer);
                    // 4) after all unique keys are found the original order of the bucket buffer is restored and the
                    //    auxiliary data file is read in chunks of delta bytes to process auxiliary data of unique keys
                    this.unsortMergeBuffer();
                    this.readAuxBucketForDispatching(writer);
                    this.dispatch();

                    LOG.debug("[{}] - [{}] - resetting disk file {}",
                              this.drumName, writer.getBucketId(), writer.getBucketId());
                    // reset the cursors of the files back to the start
                    writer.reset();

                    updateState(this.numUniqueEntries);
                }
                else
                {
                    LOG.debug("[{}] - [{}] - nothing to merge", this.drumName, writer.getBucketId());
                }
            }
            catch (Exception e)
            {
                long kvLengt = 0;
                long auxLength = 0;
                try
                {
                    kvLengt = writer.getKVFile().getFile().length();
                    auxLength = writer.getAuxFile().getFile().length();
                }
                catch (IOException ioEx)
                {
                    LOG.warn("[{}] - [{}] - Could not read bucket file due to " + ioEx.getLocalizedMessage(),
                             this.drumName, writer.getBucketId());
                }

                LOG.error("[{}] - [{}] - Error merging disk bucket files with data storage! Could not process {} key/value bytes and {} auxiliary data bytes. Reason: {}",
                          this.drumName, writer.getKVFile(), kvLengt, auxLength, e.getLocalizedMessage());
                LOG.catching(Level.ERROR, e);
            }
            finally
            {
                if (lockAcquired)
                {
                    LOG.trace("[{}] - [{}] - Releasing lock of disk bucket file", this.drumName, writer.getBucketId());
                    writer.accessDiskFile().release();
                }
                if (this.unsortingHelper != null)
                {
                    this.unsortingHelper.clear();
                }
                if (this.sortedMergeBuffer != null)
                {
                    this.sortedMergeBuffer.clear();
                }
            }
        }

        this.reset();
    }

    /**
     * Reads key/value pairs from a disk file and stores them in <code> sortedMergeBuffer</code> as {@link InMemoryEntry}
     * objects.
     *
     * @param writer
     *         The already locked writer instance we access its disk file
     *
     * @return Returns true if data could be read from the disk bucket file, false otherwise
     */
    private boolean readDataFromDiskFile(DiskWriter writer) throws DrumException
    {
        if (writer.getKVFileBytesWritten() == 0)
        {
            return false;
        }

        LOG.debug("[{}] - Reading data from disk file {}", this.drumName, writer.getKVFile().getFile());
        try
        {
            RandomAccessFile kvFile = writer.getKVFile().getFile();
            if (kvFile == null)
            {
                return false;
            }
            kvFile.seek(0);

            long writtenBytes = writer.getKVFileBytesWritten();
            LOG.debug("[{}] - [{}] - reading {} bytes from bucket file",
                      this.drumName, writer.getBucketId(), writtenBytes);

            while (kvFile.getFilePointer() < writtenBytes)
            {
                // create a new in memory data object which will hold the data of the key/value file
                InMemoryEntry<V, A> data = new InMemoryEntry<>();

                // add the data element to the list we will sort later on
                this.sortedMergeBuffer.add(data);
                // keep track of the original position
                data.setPosition(this.sortedMergeBuffer.size() - 1);

                // Set the operation of the query the key was sent with
                char c = (char) kvFile.readByte();
                DrumOperation op = DrumOperation.fromToken(c);
                data.setOperation(op);

                // Retrieve the key from the file - as it is 64-bit long, reading long should work
                long key = kvFile.readLong();
                data.setKey(key);

                int valueSize = kvFile.readInt();
                byte[] byteValue = null;
                if (valueSize > 0)
                {
                    byteValue = new byte[valueSize];
                    kvFile.read(byteValue);
                    V value = DrumUtils.deserialize(byteValue, this.valueClass);
                    data.setValue(value);
                }
                LOG.debug("[{}] - [{}] - read from bucket file - " + "operation: '{}', key: '{}', value.length: '{}' " +
                          "valueBytes: '{}'",
                          this.drumName, writer.getBucketId(), op, key, valueSize, Arrays.toString(byteValue));
            }
            this.unsortingHelper = new ArrayList<>(this.sortedMergeBuffer.size());
            kvFile.setLength(0);
        }
        catch (Exception e)
        {
            throw new DrumException(
                    "Error during reading key/values from bucket file! Reason: " + e.getLocalizedMessage(), e);
        }

        return true;
    }

    /**
     * Sorts the merge buffer based on the key-value.
     */
    private void sortMergeBuffer()
    {
        LOG.debug("[{}] - sorting merge buffer", this.drumName);
        Collections.sort(this.sortedMergeBuffer, new KeyComparator<>());
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
     */
    protected abstract void compareDataWithDataStore(List<? extends DrumStoreEntry<V>> data)
            throws DrumException, NotAppendableException;

    /**
     * Sets the cursor of the data store back to the start.
     */
    protected abstract void reset();

    /**
     * Reverts the origin order of the merge buffer. {@link #unsortingHelper} afterwards contains the origin position of
     * the merge buffer, while the {@link #sortedMergeBuffer} is not touched!
     */
    private void unsortMergeBuffer()
    {
        LOG.debug("[{}] - unsorting merge buffer", this.drumName);
        // When elements were read into the merge buffer their original positions were stored. Now I use those positions
        // as keys to "sort" them back in linear time. Traversing unsortingHelper gives the indexes into
        // sortedMergeBuffer considering the original order.
        int total = this.sortedMergeBuffer.size();
        for (int i = 0; i < total; ++i)
        {
            this.unsortingHelper.add(i);
        }
        for (int i = 0; i < total; ++i)
        {
            InMemoryEntry<V, A> data = this.sortedMergeBuffer.get(i);
            int original = data.getPosition();
            this.unsortingHelper.set(original, i);
        }
    }

    /**
     * Reads a certain bucket containing auxiliary data from disk into memory. This method uses the previously
     * initialized <em>unsortingHelper</em> list to get the proper auxiliary data after the key/value pairs got sorted
     * previously. The auxiliary data is then added to the respective key/value data object contained in the sorted
     * merge buffer.
     *
     * @param writer
     *         The already locked writer instance we access its disk file
     */
    private void readAuxBucketForDispatching(DiskWriter writer) throws DrumException
    {
        // get the number of bytes written since the last merge
        long auxWritten = writer.getAuxFileBytesWritte();
        if (auxWritten == 0)
        {
            return;
        }
        // open the bucket file for auxiliary data with a certain ID
        try
        {
            LOG.debug("[{}] - [{}] - reading auxiliary bucket '{}' for dispatching", this.drumName,
                      writer.getBucketId(), writer.getAuxFile().getFile());
            int i = 0;
            RandomAccessFile auxFile = writer.getAuxFile().getFile();
            if (auxFile == null)
            {
                return;
            }
            auxFile.seek(0);

            while (auxFile.getFilePointer() < auxWritten && i < this.unsortingHelper.size())
            {
                // check if there is data available to store the auxiliary information to
                if (null == this.unsortingHelper.get(i))
                {
                    continue;
                }
                int index = unsortingHelper.get(i);
                InMemoryEntry<V, A> data = this.sortedMergeBuffer.get(index);

                // the first value written to the cache is always the size of bytes written to the file - this is a
                // 32 bit integer value in Java!
                int auxSize = auxFile.readInt();
                // next the bytes of the actual auxiliary data are reserved and read from the file
                byte[] byteAux = new byte[auxSize];
                auxFile.read(byteAux);
                if (auxSize > 0)
                {
                    // transform the byte-array into a valid Java object of type A
                     A aux = DrumUtils.deserialize(byteAux, this.auxClass);
                    // ... and add it to the auxiliary object created before
                    data.setAuxiliary(aux);
                    LOG.debug("[{}] - [{}] - read aux data: {}", this.drumName, writer.getBucketId(), aux);
                }
                else
                {
                    LOG.debug("[{}] - [{}] - no data to read from auxiliary data bucket file", this.drumName,
                              writer.getBucketId());
                    data.setAuxiliary(null);
                }

                i++; // index variable for the unsorting helper
            }
            auxFile.setLength(0);

        }
        catch (Exception e)
        {
            throw new DrumException("Could not read auxiliary bucket file! Reason: " + e.getLocalizedMessage(), e);
        }
    }

    /**
     * Dispatches {@link DrumResult}s to the caller.
     */
    private void dispatch() throws DrumException
    {
        LOG.debug("[{}] - dispatching results", this.drumName);
        // get the number of data to dispatch
        int total = this.sortedMergeBuffer.size();
        for (int i = 0; i < total; ++i)
        {
            // check if there is data available to dispatch
            if (null == this.unsortingHelper.get(i))
            {
                continue;
            }
            // if so, get the element index of the original order
            int dataIndex = this.unsortingHelper.get(i);
            // ... and use it to retrieve the actual key/value-pair from the sorted list
            InMemoryEntry<V, A> data = this.sortedMergeBuffer.get(dataIndex);

            // now retrieving the key, the action, the result of the query and the auxiliary data of the key is fairly
            // easy
            Long key = data.getKey();
            DrumOperation op = data.getOperation();
            DrumResult result = data.getResult();
            A aux = data.getAuxiliary();

            LOG.debug("[{}] - dispatching: op: {}; key: {}; value: {}; aux: {}; result: {}", this.drumName,
                      data.getOperation(), data.getKey(), data.getValue(), data.getAuxiliary(), data.getResult());

            // inform the dispatcher of the outcome
            if (DrumOperation.CHECK.equals(op) && DrumResult.UNIQUE_KEY.equals(result))
            {
                this.dispatcher.uniqueKeyCheck(key, aux);
            }
            else
            {
                V value = data.getValue();

                if (DrumOperation.CHECK.equals(op) && DrumResult.DUPLICATE_KEY.equals(result))
                {
                    this.dispatcher.duplicateKeyCheck(key, value, aux);
                }
                else if (DrumOperation.CHECK_UPDATE.equals(op) && DrumResult.UNIQUE_KEY.equals(result))
                {
                    this.numUniqueEntries++;
                    this.dispatcher.uniqueKeyUpdate(key, value, aux);
                }
                else if (DrumOperation.CHECK_UPDATE.equals(op) && DrumResult.DUPLICATE_KEY.equals(result))
                {
                    this.dispatcher.duplicateKeyUpdate(key, value, aux);
                }
                else if (DrumOperation.UPDATE.equals(op) || DrumOperation.APPEND_UPDATE.equals(op))
                {
                    this.dispatcher.update(key, value, aux);
                }
                else
                {
                    throw new DrumException("Invalid action method selected on data element: " + data);
                }
            }
        }
    }
}
