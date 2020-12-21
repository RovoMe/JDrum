package at.rovo.drum.impl;

import at.rovo.drum.Broker;
import at.rovo.drum.DiskWriter;
import at.rovo.drum.DrumEventDispatcher;
import at.rovo.drum.DrumException;
import at.rovo.drum.DrumOperation;
import at.rovo.drum.Merger;
import at.rovo.drum.base.InMemoryEntry;
import at.rovo.drum.event.DiskWriterEvent;
import at.rovo.drum.event.DiskWriterState;
import at.rovo.drum.event.DiskWriterStateUpdate;
import at.rovo.drum.util.DiskFileHandle;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.Queue;

import net.jcip.annotations.GuardedBy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * <em>DiskBucketWriter</em> is a consumer in the producer-consumer pattern and it takes data stored from the in-memory
 * buffer and writes it to the attached disk bucket file.
 * <p>
 * According to the paper 'IRLbot: Scaling to 6 Billion Pages and Beyond' the data is separated into key/value and
 * auxiliary data diskFiles which are stored in the cache/'drumName' directory located inside the application directory
 * where <code>drumName</code> is the name of the current Drum instance.
 * <p>
 * The current implementation uses the blocking method <code>takeAll()</code> from {@link Broker}
 * to collect the items to write into the disk file.
 *
 * @param <V> The type of the value object
 * @param <A> The type of the auxiliary data object
 * @author Roman Vottner
 */
public class DiskBucketWriter<V extends Serializable, A extends Serializable> implements DiskWriter {
    /**
     * The logger of this class
     */
    private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    // immutable fields
    /**
     * The name of the DRUM instance
     */
    private final String drumName;
    /**
     * The bucket ID this instance reads from and writes to
     */
    private final int bucketId;
    /**
     * The size of a bucket before a merge action is invoked
     */
    private final int bucketByteSize;
    /**
     * The broker we get data to write from
     */
    private final Broker<InMemoryEntry<V, A>, V> broker;
    /**
     * The merger who takes care of merging disk diskFiles with the backing data store. It needs to be informed if it
     * should merge, which happens if the bytes written to the disk file exceeds certain limits
     */
    private final Merger merger;
    /**
     * The object responsible for updating listeners on state or statistic changes
     */
    private final DrumEventDispatcher eventDispatcher;
    /**
     * The handler of the key/value and auxiliary data diskFiles
     */
    private final DiskFileHandle diskFiles;

    // mutable fields
    /**
     * The number of bytes written into the key/value file
     */
    @GuardedBy("diskFiles.getLock()")
    private long kvBytesWritten = 0L;
    /**
     * The number of bytes written into the auxiliary data file
     */
    @GuardedBy("diskFiles.getLock()")
    private long auxBytesWritten = 0L;
    /**
     * flag if merging is required
     */
    private boolean mergeRequired = false;
    /**
     * Indicates if the thread the runnable part is running in should stop its work
     */
    private volatile boolean stopRequested = false;
    /**
     * Used to reduce multiple WAITING_FOR_MERGE_REQUEST event updates to a single update
     */
    private DiskWriterState lastState = null;

    /**
     * Creates a new instance and instantiates required fields.
     *
     * @param drumName        The name of the Drum instance
     * @param bucketId        The index of the bucket this writer will read data from or write to
     * @param bucketByteSize  The size in bytes before a merge with the backing data store is invoked
     * @param broker          The broker who administers the in memory data
     * @param merger          The {@link Merger} object that is responsible for integrating the requests into the
     *                        backing data store
     * @param eventDispatcher The object to receive event notifications
     * @param diskFiles       The {@link DiskFileHandle file handle} object providing access to the backing data store
     *                        files
     */
    public DiskBucketWriter(@Nonnull final String drumName,
                            final int bucketId,
                            final int bucketByteSize,
                            @Nonnull final Broker<InMemoryEntry<V, A>, V> broker,
                            @Nonnull final Merger merger,
                            @Nonnull final DrumEventDispatcher eventDispatcher,
                            @Nonnull final DiskFileHandle diskFiles) {
        this.drumName = drumName;
        this.bucketId = bucketId;
        this.bucketByteSize = bucketByteSize;
        this.broker = broker;
        this.merger = merger;
        this.eventDispatcher = eventDispatcher;
        this.diskFiles = diskFiles;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    ///                              executed usually by disk bucket writer thread                                   ///
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    @Override
    public void run() {
        Queue<InMemoryEntry<V, A>> elementsToPersist;
        while (!this.stopRequested) {
            try {
                if (this.lastState != DiskWriterState.WAITING_FOR_DATA) {
                    LOG.debug("[{}] - [{}] - waiting for data", this.drumName, this.bucketId);
                    this.lastState = updateState(DiskWriterState.WAITING_FOR_DATA);
                }

                // takeAll() will block till data is available
                elementsToPersist = this.broker.takeAll();

                // skip further processing if no data was available
                if (elementsToPersist.isEmpty()) {
                    continue;
                }

                this.lastState = updateState(DiskWriterState.DATA_RECEIVED);

                LOG.debug("[{}] - [{}] - received {} data elements",
                        this.drumName, this.bucketId, elementsToPersist.size());
                // write data to the disk bucket diskFiles
                this.feedBucket(elementsToPersist);
                // check if a merged is required due to exceeding the bucket byte size; if so signal a merge request!
                if (this.mergeRequired) {
                    this.merger.requestMerge();
                }

                this.mergeRequired = false;
            } catch (Exception e) {
                this.handleException(e);
                Thread.currentThread().interrupt();
            }
        }
        // In case a stop request was received while at the same time data was pushed from the producer into the buffer,
        // we need to re-check the buffer a last time to ensure that no data is lost.
        elementsToPersist = this.broker.takeAll();
        this.lastState = updateState(DiskWriterState.DATA_RECEIVED);

        LOG.debug("[{}] - [{}] - received {} data elements",
                this.drumName, this.bucketId, elementsToPersist.size());
        // write data to the disk bucket diskFiles
        try {
            this.feedBucket(elementsToPersist);
        } catch (DrumException ex) {
            this.handleException(ex);
            return;
        }

        // push the latest data which has not yet been written to the data store to be written
        if (this.kvBytesWritten > 0) {
            this.merger.requestMerge();
        }
        updateState(DiskWriterState.FINISHED);
        LOG.trace("[{}] - [{}] - stopped processing!", this.drumName, this.bucketId);
        // signal the merger thread that work is finally done
        this.merger.writerDone();
    }

    private void handleException(@Nonnull final Exception ex) {
        LOG.error("[" + this.drumName + "] - [" + this.bucketId + "] - caught exception: " + ex.getLocalizedMessage(), ex);
        updateState(DiskWriterState.FINISHED_WITH_ERROR);
        this.merger.writerDone();
    }

    /**
     * Feeds the key/value and auxiliary bucket diskFiles with the data stored in memory buffers.
     *
     * @param inMemoryData The buffer which contains the data to persist to disk
     */
    private void feedBucket(@Nonnull final Queue<InMemoryEntry<V, A>> inMemoryData) throws DrumException {
        boolean lockAcquired = false;
        try {
            if (inMemoryData.isEmpty()) {
                // no data to write
                updateState(DiskWriterState.EMPTY);
                return;
            }

            updateState(DiskWriterState.WAITING_FOR_LOCK);
            // do not interrupt the file access as otherwise data might get lost!
            this.diskFiles.getLock().acquireUninterruptibly();
            LOG.trace("[{}] - [{}] - Acquired lock of disk bucket file", this.drumName, this.bucketId);
            lockAcquired = true;
            updateState(DiskWriterState.WRITING);
            final RandomAccessFile kvFile = this.diskFiles.getKVFile();
            final RandomAccessFile auxFile = this.diskFiles.getAuxFile();

            final long kvStart = kvFile.getFilePointer();
            final long auxStart = auxFile.getFilePointer();

            LOG.trace("[{}] - [{}] - {} elements to write", this.drumName, this.bucketId, inMemoryData.size());
            for (InMemoryEntry<V, A> data : inMemoryData) {
                LOG.info("[{}] - [{}] - feeding bucket with: {}; value: {}",
                        this.drumName, this.bucketId, data.getKey(), data.getValue());
                final long kvStartPos = kvFile.getFilePointer();
                final long auxStartPos = auxFile.getFilePointer();

                // Write the following sequentially for the key/value bucket file:
                // - operation; (1 byte)
                // - key; (8 byte)
                // - value length; (4 byte)
                // - value. (variable byte)

                // write the operation

                final DrumOperation op = data.getOperation();
                final char c = op.getTokenForOperation();
                kvFile.write(c);

                // write the key
                kvFile.writeLong(data.getKey());

                // write the value
                final byte[] byteValue = data.getValueAsBytes();
                final long kvEndPos = writeBytesToFile(kvFile, byteValue);

                if (byteValue != null) {
                    LOG.info("[{}] - [{}] - wrote to kvBucket file - operation: '{}' key: '{}', value.length: '{}' " +
                                    "byteValue: '{}' and value: '{}' - bytes written " + "in total: {}",
                            this.drumName, this.bucketId, c, data.getKey(), byteValue.length,
                            Arrays.toString(byteValue), data.getValue(), (kvEndPos - kvStartPos));
                } else {
                    LOG.info("[{}] - [{}] - wrote to kvBucket file - operation: '{}' key: '{}', value.length: '0' " +
                                    "byteValue: 'null' and value: '{}' - bytes written " + "in total: {}",
                            this.drumName, this.bucketId, c, data.getKey(), data.getValue(), (kvEndPos - kvStartPos));
                }

                // Write the following sequentially for the auxiliary data bucket file:
                // - aux length; (4 byte)
                // - aux. (variable byte)

                final byte[] byteAux = data.getAuxiliaryAsBytes();
                final long auxEndPos = writeBytesToFile(auxFile, byteAux);

                if (byteAux != null) {
                    LOG.info("[{}] - [{}] - wrote to auxBucket file - aux.length: '{}' byteAux: '{}' and aux: '{}' - "
                                    + "bytes written in total: {}",
                            this.drumName, this.bucketId, byteAux.length, Arrays.toString(byteAux),
                            data.getAuxiliary(), (auxEndPos - auxStartPos));
                } else {
                    LOG.info("[{}] - [{}] - wrote to auxBucket file - aux.length: '0' byteAux: 'null' and aux: '{}' - "
                                    + "bytes written in total: {}",
                            this.drumName, this.bucketId, data.getAuxiliary(), (auxEndPos - auxStartPos));
                }
            }

            this.kvBytesWritten += (kvFile.getFilePointer() - kvStart);
            this.auxBytesWritten += (auxFile.getFilePointer() - auxStart);

            updateState(this.kvBytesWritten, this.auxBytesWritten);

            // is it merge time?
            if (this.kvBytesWritten > this.bucketByteSize || this.auxBytesWritten > this.bucketByteSize) {
                LOG.info("[{}] - [{}] - requesting merge", this.drumName, this.bucketId);
                this.mergeRequired = true;
            }
        } catch (Exception e) {
            throw new DrumException("Error feeding bucket " + this.drumName + "-" + this.bucketId + "!", e);
        } finally {
            if (lockAcquired) {
                LOG.trace("[{}] - [{}] - Releasing lock of disk bucket file", this.drumName, this.bucketId);
                this.diskFiles.getLock().release();
            }
        }
    }

    /**
     * Writes the bytes contained in the provided <em>bytes</em> argument to the provided <em>file</em> by first
     * writing the length of the bytes to the file followed by the actual payload of the bytes array.
     *
     * @param file  The file to write the data to
     * @param bytes The payload to write
     * @return The current position of the pointer after the write operation
     * @throws IOException Thrown if any exception occurred during the attempt to write the payload to the file
     */
    private long writeBytesToFile(@Nonnull final RandomAccessFile file, @Nullable final byte[] bytes) throws IOException {
        if (bytes != null) {
            file.writeInt(bytes.length);
            file.write(bytes);
        } else {
            file.writeInt(0);
        }

        return file.getFilePointer();
    }

    /**
     * Generates an {@link DiskWriterStateUpdate} event for the provided state.
     *
     * @param newState The new state this instance is in
     * @return The new state of this instance
     */
    private DiskWriterState updateState(@Nonnull final DiskWriterState newState) {
        this.eventDispatcher.update(new DiskWriterStateUpdate(this.drumName, this.bucketId, newState));
        return newState;
    }

    /**
     * Generates an {@link DiskWriterEvent} for the given <em>byteLengthKV</em> and <em>byteLengthAux</em> values.
     *
     * @param byteLengthKV  The length of the key-value pair bytes
     * @param byteLengthAux The length of the auxiliary data bytes
     */
    private void updateState(final long byteLengthKV, final long byteLengthAux) {
        this.eventDispatcher.update(new DiskWriterEvent(this.drumName, this.bucketId, byteLengthKV, byteLengthAux));
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    ///                                executed usually by the merger thread                                         ///
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    @Override
    public int getBucketId() {
        return this.bucketId;
    }

    @Override
    @Nonnull
    public DiskFileHandle getDiskFiles() {
        return this.diskFiles;
    }

    @Override
    public long getKVFileBytesWritten() {
        return this.kvBytesWritten;
    }

    @Override
    public long getAuxFileBytesWritten() {
        return this.auxBytesWritten;
    }

    @Override
    public void reset() {
        // set the bytes written to 0
        this.kvBytesWritten = 0L;
        this.auxBytesWritten = 0L;

        // set the file pointer to the start of the file
        try {
            this.diskFiles.reset();
        } catch (IOException e) {
            LOG.error("Error while resetting the disk bucket pointers of " + this.drumName + "-" + this.bucketId + "!",
                    e);
        }

        updateState(this.kvBytesWritten, this.auxBytesWritten);
        updateState(DiskWriterState.EMPTY);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    ///                                executed usually by the main thread                                           ///
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    @Override
    public void stop() {
        this.stopRequested = true;
        LOG.trace("[{}] - [{}] - stop requested!", this.drumName, this.bucketId);
    }

    @Override
    public void close() {
        try {
            this.diskFiles.close();
        } catch (IOException e) {
            LOG.error("Error while closing disk bucket writer " + this.drumName + "-" + this.bucketId + "!", e);
        } finally {
            LOG.debug("[{}] - [{}] - Closed disk bucket files successfully", this.drumName, this.bucketId);
        }
    }
}
