package at.rovo.caching.drum.internal;

import at.rovo.caching.drum.Broker;
import at.rovo.caching.drum.data.ByteSerializer;
import at.rovo.caching.drum.event.DrumEventDispatcher;
import at.rovo.caching.drum.event.InMemoryBufferEvent;
import at.rovo.caching.drum.event.InMemoryBufferState;
import at.rovo.caching.drum.event.InMemoryBufferStateUpdate;
import at.rovo.caching.drum.util.lockfree.FlippableData;
import at.rovo.caching.drum.util.lockfree.FlippableDataContainer;
import java.lang.invoke.MethodHandles;
import java.util.Queue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * <em>InMemoryMessageBroker</em> is a {@link at.rovo.caching.drum.Broker} implementation which manages {@link
 * InMemoryData} objects.
 * <p>
 * This implementation buffers data into an active buffer while an attached consumer (IDiskWriter) instance is writing
 * the data stored in the back-buffer.
 * <p>
 * A value representing the admissible byte size can be provided which sends an event if the bytes stored in the active
 * buffer exceeds this value. The value of the byte size can be set on invoking the constructor.
 *
 * @param <T>
 *         The type of the data the broker manages
 * @param <V>
 *         The type of the value
 * @param <A>
 *         The type of the auxiliary data attached to a key
 *
 * @author Roman Vottner
 */
public class InMemoryMessageBroker<T extends InMemoryData<V, A>, V extends ByteSerializer<V>, A extends ByteSerializer<A>>
        implements Broker<T, V, A>
{
    /** The logger of this class **/
    private final static Logger LOG = LogManager.getLogger(MethodHandles.lookup().lookupClass());

    /** The name of the DRUM instance **/
    private String drumName = null;
    /** The object responsible for updating listeners on state or statistic changes **/
    private DrumEventDispatcher eventDispatcher = null;
    /** The ID of the buffer **/
    private int bucketId = 0;
    /** The flippable queue to add in memory data to **/
    private FlippableDataContainer<T> queue = new FlippableDataContainer<>();
    /** The size of the buffer before the two buffers get exchanged and the results being available through
     * <code>takeAll</code> **/
    private int byteSizePerBuffer = 0;
    /** The old state of the crawler. Used to minimize state updates if the state remained the same as the old state **/
    private InMemoryBufferState oldState = null;

    /** The lock object is needed to let consumers wait on invoking {@link #takeAll()} if no data is available **/
    private Lock lock = new ReentrantLock();
    /** Informs a waiting thread, which invoked await() previously, that data is available for writing to disk bucket
     * through invoking signal() **/
    private Condition dataAvailable = lock.newCondition();
    /** Flag to indicate that the data is available **/
    private volatile boolean isDataAvailable = false;

    /** Indicates if the thread the runnable part is running in should stop its work **/
    private volatile boolean stopRequested = false;
    /** To avoid logging of multiple stopped sending data messages **/
    private boolean stopAlreadyLogged = false;

    /**
     * Creates a new instance and initializes necessary fields.
     *
     * @param drumName
     *         The name of the drum instance. This value is only required to log more appropriate and therefore
     *         traceable statements
     * @param id
     *         The bucket identifier this broker will act on
     * @param byteSizePerBuffer
     *         The length of the buffer in size upon which the consumer will get the buffered data
     * @param eventDispatcher
     *         A reference to the event dispatcher in order to inform listeners about state changes on the broker
     */
    public InMemoryMessageBroker(String drumName, int id, int byteSizePerBuffer, DrumEventDispatcher eventDispatcher)
    {
        this.drumName = drumName;
        this.eventDispatcher = eventDispatcher;
        this.bucketId = id;
        this.byteSizePerBuffer = byteSizePerBuffer;

        // the old state used to filter multiple state updates on the same state
        this.oldState = InMemoryBufferState.EMPTY;
        this.eventDispatcher
                .update(new InMemoryBufferStateUpdate(this.drumName, this.bucketId, InMemoryBufferState.EMPTY));
    }


    @Override
    public void put(T data)
    {
        if (null == data)
        {
            return;
        }

        FlippableData<T> _data = this.queue.put(data);
        this.isDataAvailable = true;

        LOG.info("[{}] - [{}] - Received data-object: {}; value: {}; aux: {} for operation: {}", this.drumName,
                 this.bucketId, data.getKey(), data.getValue(), data.getAuxiliary(), data.getOperation());

       int byteLengthKV = _data.getKeyLength() + _data.getValLength();
       int byteLengthAux = _data.getAuxLength();

        this.eventDispatcher.update(
                new InMemoryBufferEvent(this.drumName, this.bucketId, byteLengthKV, byteLengthAux));

        this.checStateChange(byteLengthKV, byteLengthAux);

        this.signalDataAvailable();
    }

    private void checStateChange(int byteLengthKV, int byteLengthAux) {
        if ((byteLengthKV > this.byteSizePerBuffer ||
             byteLengthAux > this.byteSizePerBuffer))
        {
            if (!InMemoryBufferState.EXCEEDED_LIMIT.equals(this.oldState))
            {
                this.oldState = InMemoryBufferState.EXCEEDED_LIMIT;
                this.eventDispatcher.update(
                        new InMemoryBufferStateUpdate(this.drumName, this.bucketId,
                                                      InMemoryBufferState.EXCEEDED_LIMIT));
            }
        }
        else
        {
            if (!InMemoryBufferState.WITHIN_LIMIT.equals(this.oldState))
            {
                this.oldState = InMemoryBufferState.WITHIN_LIMIT;
                this.eventDispatcher.update(
                        new InMemoryBufferStateUpdate(this.drumName, this.bucketId,
                                                      InMemoryBufferState.WITHIN_LIMIT));
            }
        }
    }

    private void signalDataAvailable() {
        try
        {
            this.lock.lock();
            this.dataAvailable.signal();
        }
        finally
        {
            this.lock.unlock();
        }
    }

    @Override
    public Queue<T> takeAll() throws InterruptedException
    {
        if (!this.isDataAvailable && this.stopRequested)
        {
            if (!this.stopAlreadyLogged)
            {
                LOG.trace("[{}] - [{}] - stopped sending data!", this.drumName, this.bucketId);
                this.stopAlreadyLogged = true;
            }
            return null;
        }

        try
        {
            this.lock.lock();

            if (!this.isDataAvailable)
            {
                // wait till data is available
                this.dataAvailable.await();
            }

            Queue<T> queue = this.queue.flip();
            this.isDataAvailable = false;
            LOG.debug("[{}] - [{}] - transmitting data objects", this.drumName, this.bucketId);
            if (LOG.isTraceEnabled())
            {
                queue.forEach(entry -> LOG.trace("Transmitted: ", entry));
            }

            return queue;
        }
        finally
        {
            this.lock.unlock();
        }
    }

    @Override
    public void stop()
    {
        LOG.trace("[{}] - [{}] - stop requested!", this.drumName, this.bucketId);
        this.stopRequested = true;

        try
        {
            this.lock.lock();
            // if a consumer thread is waiting for data and we need to shutdown, we invoke the currently blocked
            // consumer thread in order to shut down the broker correctly. Signaling a blocked thread however requires
            // to be done while a lock is active otherwise an illegal monitor exception is thrown!
            this.dataAvailable.signal();
        }
        finally
        {
            this.lock.unlock();
        }
    }
}
