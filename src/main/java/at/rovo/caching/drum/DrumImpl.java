package at.rovo.caching.drum;

import at.rovo.caching.drum.data.ByteSerializer;
import at.rovo.caching.drum.event.DrumEventDispatcher;
import at.rovo.caching.drum.internal.DiskBucketWriter;
import at.rovo.caching.drum.internal.InMemoryData;
import at.rovo.caching.drum.internal.InMemoryMessageBroker;
import at.rovo.caching.drum.internal.backend.DrumStorageFactory;
import at.rovo.caching.drum.util.DrumExceptionHandler;
import at.rovo.caching.drum.util.DrumUtils;
import at.rovo.caching.drum.util.NamedThreadFactory;
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
 * store and process input received by its <ul> <li>{@link #check(Long)} or {@link #check(Long, A)}</li> <li>{@link
 * #update(Long, V)} or {@link #update(Long, V, A)}</li> <li>{@link #checkUpdate(Long, V)} or {@link #checkUpdate(Long,
 * V, A)}</li> </ul> methods.
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
@SuppressWarnings("unused")
public class DrumImpl<V extends ByteSerializer<V>, A extends ByteSerializer<A>> implements Drum<V, A>
{
    /** The logger of this class **/
    private final static Logger LOG = LogManager.getLogger(MethodHandles.lookup().lookupClass());

    /** The name of the DRUM instance **/
    protected String drumName = null;
    /** The number of buffers and buckets used **/
    protected int numBuckets = 0;
    /** The size of an in-memory buffer **/
    protected int bufferSize = 0;
    /**
     * The broker list which holds elements in memory until they get written to the disk file
     **/
    protected List<Broker<InMemoryData<V, A>, V, A>> inMemoryBuffer = null;
    /**
     * The set of writer objects that listens to notifications of a broker and write content from the broker to a disk
     * file
     **/
    protected List<DiskWriter<V, A>> diskWriters = null;
    /**
     * The object that compares keys of data-objects for their uniqueness and merges them into the data store in case
     * the need to be updated
     **/
    protected Merger<V, A> merger = null;
    /** The execution service which hosts our threads **/
    protected ExecutorService executor = null;
    /** The merger thread **/
    protected Thread mergerThread = null;
    /**
     * The event dispatcher used to inform listeners of internal state changes and certain statistics
     **/
    protected DrumEventDispatcher eventDispatcher = new DrumEventDispatcher();
    /** The event dispatcher thread **/
    protected Thread eventDispatcherThread = null;

    /**
     * Creates a new instance and assigns initial values contained within the builder object to the corresponding
     * attributes.
     *
     * @param builder
     *         The builder object which contains the initialization parameters specified by the invoker
     *
     * @throws DrumException
     *         If during the initialization of the backing data store an error occurred
     */
    DrumImpl(DrumBuilder<V, A> builder) throws DrumException
    {
        if (builder.getListener() != null)
        {
            this.addDrumListener(builder.getListener());
        }

        DrumStorageFactory<V, A> factory;
        if (builder.getFactory() != null)
        {
            factory = builder.getFactory();
        }
        else
        {
            factory = DrumStorageFactory
                    .getDefaultStorageFactory(builder.getDrumName(), builder.getNumBuckets(), builder.getDispatcher(),
                                              builder.getValueClass(), builder.getAuxClass(), this.eventDispatcher);
        }

        this.init(builder.getDrumName(), builder.getNumBuckets(), builder.getBufferSize(), builder.getDispatcher(),
                  builder.getValueClass(), builder.getAuxClass(), factory);
    }


    /**
     * Initializes the DRUM instance with required data and starts the worker threads.
     *
     * @param drumName
     *         The name of the DRUM instance
     * @param numBuckets
     *         The number of buckets to be used
     * @param bufferSize
     *         The size of a single buffer in bytes
     * @param dispatcher
     *         The {@link Dispatcher} implementation which will receive information on items added via
     *         <code>check</code>, <code>update</code> or <code>checkUpdate</code>.
     * @param valueClass
     *         The class-type of the value for a certain key
     * @param auxClass
     *         The auxiliary data-type attached to a certain key
     * @param factory
     *         The factory object which defines where data should be stored in. Note that factory must return an
     *         implementation of IMerger
     *
     * @throws DrumException
     */
    private void init(String drumName, int numBuckets, int bufferSize, Dispatcher<V, A> dispatcher,
                      Class<? super V> valueClass, Class<? super A> auxClass, DrumStorageFactory<V, A> factory)
            throws DrumException
    {
        this.eventDispatcherThread = new Thread(this.eventDispatcher);
        this.eventDispatcherThread.setName(drumName + "EventDispatcher");
        this.eventDispatcherThread.start();

        this.drumName = drumName;
        this.numBuckets = numBuckets;
        this.bufferSize = bufferSize;

        // create the broker and the consumer listening to the broker
        this.inMemoryBuffer = new ArrayList<>(numBuckets);
        this.diskWriters = new ArrayList<>(numBuckets);
        this.merger = factory.getStorage();

        DrumExceptionHandler exceptionHandler = new DrumExceptionHandler();
        NamedThreadFactory writerFactory = new NamedThreadFactory();
        writerFactory.setName(this.drumName + "-Writer");
        writerFactory.setUncaughtExceptionHanlder(exceptionHandler);
        this.executor = Executors.newFixedThreadPool(this.numBuckets, writerFactory);

        for (int i = 0; i < numBuckets; i++)
        {
            Broker<InMemoryData<V, A>, V, A> broker =
                    new InMemoryMessageBroker<>(drumName, i, bufferSize, this.eventDispatcher);
            DiskWriter<V, A> consumer =
                    new DiskBucketWriter<>(drumName, i, bufferSize, broker, this.merger, this.eventDispatcher);

            this.inMemoryBuffer.add(broker);
            this.diskWriters.add(consumer);

            this.executor.submit(consumer);

            // add a reference of the disk writer to the merger, so it can use
            // the semaphore to lock the file it is currently reading from to
            // merge the data into the backing data store.
            // While reading from a file, a further access to the file (which
            // should result in a write access) is therefore refused.
            this.merger.addDiskFileWriter(consumer);
        }
        this.mergerThread = new Thread(this.merger, this.drumName + "-Merger");
        // this.mergerThread.setPriority(Math.min(10,
        // this.mergerThread.getPriority()+1));
        //this.mergerThread.setUncaughtExceptionHandler(exceptionHandler);
        this.mergerThread.start();
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
        LOG.debug("[{}] - Disposal initiated", this.drumName);
        // flip the buffers which sends the writers the latest data
        this.inMemoryBuffer.forEach(Broker::stop);

        // give the threads a chance to finish their work without being
        // interrupted
        this.diskWriters.forEach(DiskWriter::stop);

        this.executor.shutdown();

        // wait for the threads to finish
        try
        {
            this.executor.awaitTermination(1, TimeUnit.MINUTES);
        }
        catch (InterruptedException e)
        {
            LOG.error("Error while terminating threads", e);
        }

        this.merger.stop();
        try
        {
            this.mergerThread.join();
        }
        catch (InterruptedException e)
        {
            LOG.error("Error while terminating merger threads", e);
        }

        // close the open resources held by the writers
        this.diskWriters.forEach(DiskWriter::close);

        this.eventDispatcher.stop();
        this.eventDispatcherThread.interrupt();
        LOG.trace("[{}] - disposed", this.drumName);
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
     */
    private void add(Long key, V value, A aux, DrumOperation operation)
    {
        // get the bucket index based on the first n bits of the key, according
        // to the number of defined buckets
        int bucketId = DrumUtils.getBucketForKey(key, this.numBuckets);

        // add a new InMemoryData object to the broker
        this.inMemoryBuffer.get(bucketId).put(new InMemoryData<>(key, value, aux, operation));
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
