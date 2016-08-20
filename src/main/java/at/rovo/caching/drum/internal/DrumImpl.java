package at.rovo.caching.drum.internal;

import at.rovo.caching.drum.Broker;
import at.rovo.caching.drum.DiskWriter;
import at.rovo.caching.drum.Drum;
import at.rovo.caching.drum.DrumEventDispatcher;
import at.rovo.caching.drum.DrumException;
import at.rovo.caching.drum.DrumListener;
import at.rovo.caching.drum.DrumOperation;
import at.rovo.caching.drum.Merger;
import at.rovo.caching.drum.util.DrumExceptionHandler;
import at.rovo.caching.drum.util.DrumUtils;
import at.rovo.caching.drum.util.NamedThreadFactory;
import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * This implementation of the 'Disk Repository with Update Management' structure utilizes a consumer/producer pattern to
 * store and process input received by its
 * <ul>
 *     <li>{@link #check(Long)} or {@link #check(Long, A)}</li>
 *     <li>{@link #update(Long, V)} or {@link #update(Long, V, A)}</li>
 *     <li>{@link #checkUpdate(Long, V)} or {@link #checkUpdate(Long, V, A)}</li>
 * </ul>
 * methods.
 * <p>
 * Internally <code>numBuckets</code> buffers and buckets will be created which will hold the data sent to DRUM. Buffers
 * are the in-memory storage while buckets are the intermediary disk files. Buffers fill up to <code> bufferSize</code>
 * bytes before they get sent to a disk file.
 *
 * @param <V>
 *         The type of the value
 * @param <A>
 *         The type of the auxiliary data attached to a key
 *
 * @author Roman Vottner
 */
public class DrumImpl<V extends Serializable, A extends Serializable> implements Drum<V, A>
{
    /** The logger of this class **/
    private final static Logger LOG = LogManager.getLogger(MethodHandles.lookup().lookupClass());

    /** The name of the DRUM instance **/
    protected final String drumName;
    /** The number of buffers and buckets used **/
    protected final int numBuckets;

    /** The broker list which holds elements in memory until they get written to the disk file **/
    private final List<Broker<InMemoryEntry<V, A>, V>> inMemoryBuffer;
    /** The set of writer objects that listens to notifications of a broker and write content from the broker to a disk
     * file **/
    private final List<DiskWriter> diskWriters;
    /** The merger which takes care of synchronizing the data stored in bucket disk files with a backing data store **/
    private final Merger merger;
    /** The instance which takes care of informing registered listeners about internal state changes **/
    private final DrumEventDispatcher eventDispatcher;

    /** The execution service which hosts our writer, merger and event dispatcher threads **/
    private final ExecutorService executors;

    /**
     * Creates a new instance and assigns initial values contained within the builder object to the corresponding
     * attributes.
     *
     * @param settings
     *         The parameter object containing the actual settings for the DRUM instance
     *
     * @throws DrumException
     *         If during the initialization of the backing data store an error occurred
     */
    public DrumImpl(DrumSettings<V, A> settings) throws DrumException
    {
        this.drumName = settings.getDrumName();
        this.numBuckets = settings.getNumBuckets();
        this.eventDispatcher = settings.getEventDispatcher();
        DrumExceptionHandler exceptionHandler = new DrumExceptionHandler();

        // create the broker and the consumer listening to the broker
        this.inMemoryBuffer = new ArrayList<>(numBuckets);
        this.diskWriters = new ArrayList<>(numBuckets);
        this.merger = settings.getFactory().getStorage();

        // Bucket-writer-threads
        NamedThreadFactory namedFactory = new NamedThreadFactory();
        namedFactory.setName(this.drumName + "-Writer");
        namedFactory.setUncaughtExceptionHandler(exceptionHandler);
        this.executors = Executors.newFixedThreadPool(this.numBuckets + 2, namedFactory);

        for (int i = 0; i < numBuckets; i++)
        {
            Broker<InMemoryEntry<V, A>, V> broker =
                    new InMemoryMessageBroker<>(drumName, i, settings.getBufferSize(), eventDispatcher);
            DiskWriter consumer =
                    new DiskBucketWriter<>(drumName, i, settings.getBufferSize(), broker, merger, eventDispatcher);

            this.inMemoryBuffer.add(broker);
            this.diskWriters.add(consumer);

            this.executors.execute(consumer);

            // add a reference of the disk writer to the merger, so it can use the semaphore to lock the file it is
            // currently reading from to merge the data into the backing data store. While reading from a file, a
            // further access to the file (which should result in a write access) is therefore refused.
            this.merger.addDiskFileWriter(consumer);
        }
        // Merger-thread
        namedFactory.setName(this.drumName + "-Merger");
        this.executors.execute(this.merger);

        // Dispatcher-thread
        namedFactory.setName(this.drumName + "-EventDispatcher");
        this.executors.execute(this.eventDispatcher);

        if (settings.getListener() != null)
        {
            this.addDrumListener(settings.getListener());
        }

        // do not allow any more threads to be added to the thread-pool
        this.executors.shutdown();
    }

    @Override
    public void check(Long key)
    {
        this.add(key, null, null, DrumOperation.CHECK);
    }

    @Override
    public void check(Long key, A aux)
    {
        this.add(key, null, aux, DrumOperation.CHECK);
    }

    @Override
    public void update(Long key, V value)
    {
        this.add(key, value, null, DrumOperation.UPDATE);
    }

    @Override
    public void update(Long key, V value, A aux)
    {
        this.add(key, value, aux, DrumOperation.UPDATE);
    }

    @Override
    public void appendUpdate(Long key, V value)
    {
        this.add(key, value, null, DrumOperation.APPEND_UPDATE);
    }

    @Override
    public void appendUpdate(Long key, V value, A aux)
    {
        this.add(key, value, aux, DrumOperation.APPEND_UPDATE);
    }

    @Override
    public void checkUpdate(Long key, V value)
    {
        this.add(key, value, null, DrumOperation.CHECK_UPDATE);
    }

    @Override
    public void checkUpdate(Long key, V value, A aux)
    {
        this.add(key, value, aux, DrumOperation.CHECK_UPDATE);
    }

    @Override
    public void dispose() throws DrumException
    {
        Exception caught = null;

        LOG.debug("[{}] - Disposal initiated", this.drumName);
        // give the threads a chance to finish their work without being interrupted
        // flip the buffers which sends the writers the latest data
        this.inMemoryBuffer.forEach(Broker::stop);
        // write the last data collected to the bucket files
        this.diskWriters.forEach(DiskWriter::stop);
        // merge the content of the bucket files with the data store and send the last unique/duplicate key results
        this.merger.stop();
        // send the remaining internal state updates to listeners
        this.eventDispatcher.stop();

        // wait for the threads to finish
        try
        {
            this.executors.awaitTermination(10, TimeUnit.SECONDS);
        }
        catch (InterruptedException e)
        {
            LOG.error("Error while terminating threads", e);
            caught = e;
        }

        // close the open resources held by the writers
        this.diskWriters.forEach(DiskWriter::close);
        // ... and the merger
        this.merger.close();

        LOG.trace("[{}] - disposed", this.drumName);

        if (caught != null)
        {
            throw new DrumException(caught.getLocalizedMessage(), caught);
        }
    }

    @Override
    public void addDrumListener(DrumListener listener)
    {
        this.eventDispatcher.addDrumListener(listener);
    }

    @Override
    public void removeDrumListener(DrumListener listener)
    {
        this.eventDispatcher.removeDrumListener(listener);
    }

    /**
     * Stores the key, the value and the auxiliary data as well as the operation to be executed on these data in the
     * according in-memory buffer.
     *
     * @param key
     *         The hash value of the data
     * @param value
     *         The value associated with the key
     * @param aux
     *         The auxiliary data of the key
     * @param operation
     *         The operation to be used on the data
     *
     * @throws IllegalArgumentException
     *         If the key is not a power of 2
     * @throws IllegalStateException
     *         If an entry is added after {@link #dispose()} was invoked
     */
    private void add(Long key, V value, A aux, DrumOperation operation)
    {
        // get the bucket index based on the first n bits of the key, according to the number of defined buckets
        int bucketId = DrumUtils.getBucketForKey(key, this.numBuckets);

        // add a new InMemoryData object to the broker
        this.inMemoryBuffer.get(bucketId).put(new InMemoryEntry<>(key, value, aux, operation));
    }

    /**
     * Returns the name of the DRUM instance.
     *
     * @return The name of the DRUM instance
     */
    public String getName()
    {
        return this.drumName;
    }

    /**
     * Returns the number of buckets used by this DRUM instance.
     *
     * @return The number of buckets used
     */
    public int getNumberOfBuckets()
    {
        return this.numBuckets;
    }
}
