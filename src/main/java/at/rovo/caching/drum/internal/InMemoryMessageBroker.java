package at.rovo.caching.drum.internal;

import at.rovo.caching.drum.Broker;
import at.rovo.caching.drum.data.ByteSerializer;
import at.rovo.caching.drum.event.DrumEventDispatcher;
import at.rovo.caching.drum.event.InMemoryBufferEvent;
import at.rovo.caching.drum.event.InMemoryBufferState;
import at.rovo.caching.drum.event.InMemoryBufferStateUpdate;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
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
    /**
     * The object responsible for updating listeners on state or statistic changes
     **/
    private DrumEventDispatcher eventDispatcher = null;
    /** The ID of the buffer **/
    private int bucketId = 0;
    /** The index of the currently active buffer **/
    private AtomicInteger activeBuffer = new AtomicInteger(0);
    /**
     * Will contain the actual buffer as well as the length of the key/value and auxiliary queue
     */
    @SuppressWarnings("unchecked")
    private MemoryBuffer<T>[] buffers = (MemoryBuffer<T>[]) Array.newInstance(MemoryBuffer.class, 2);

    /**
     * The size of the buffer before the two buffers get exchanged and the results being available through
     * <code>takeAll</code>
     **/
    private int byteSizePerBuffer = 0;

    /**
     * Indicates if the thread the runnable part is running in should stop its work
     **/
    private volatile boolean stopRequested = false;
    /**
     * The old state of the crawler. Used to minimize state updates if the state remained the same as the old state
     **/
    private InMemoryBufferState oldState = null;

    /** The lock object used to synchronize the threads **/
    private Lock lock = new ReentrantLock();
    /**
     * Informs a waiting thread, which invoked await() previously, that data is available for writing to disk bucket
     * through invoking signal().
     **/
    private Condition dataAvailable = lock.newCondition();

    /** Flag to indicate that the data is available **/
    private volatile boolean isDataAvailable = false;

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

        this.buffers[0] = new MemoryBuffer<>(new ConcurrentLinkedQueue<>(), 0, 0);
        this.buffers[1] = new MemoryBuffer<>(new ConcurrentLinkedQueue<>(), 0, 0);

        // the old state used to filter multiple state updates on the same state
        this.oldState = InMemoryBufferState.EMPTY;
        this.eventDispatcher
                .update(new InMemoryBufferStateUpdate(this.drumName, this.bucketId, InMemoryBufferState.EMPTY));
    }


    @Override
    public void put(T data)
    {
        int activeQueue = this.activeBuffer.get();
        MemoryBuffer<T> activeBuffer = this.buffers[activeQueue];
        activeBuffer.getBuffer().add(data);

        LOG.info("[{}] - [{}] - [{}] - Received data-object: {}; value: {}; aux: {} for operation: {}", this.drumName,
                 this.bucketId, activeQueue, data.getKey(), data.getValue(), data.getAuxiliary(), data.getOperation());

        int keyLength = data.getKeyAsBytes().length;
        int valLength = 0;
        if (data.getValue() != null)
        {
            valLength = data.getValueAsBytes().length;
        }
        int auxLength = 0;
        if (data.getAuxiliary() != null)
        {
            auxLength = data.getAuxiliaryAsBytes().length;
        }

        activeBuffer.addByteLengthKV(keyLength + valLength);
        activeBuffer.addByteLengthAux(auxLength);

        this.eventDispatcher
                .update(new InMemoryBufferEvent(this.drumName, this.bucketId, activeBuffer.getByteLengthKV(),
                                                activeBuffer.getByteLengthAux()));

        if ((activeBuffer.getByteLengthKV() > this.byteSizePerBuffer ||
             activeBuffer.getByteLengthAux() > this.byteSizePerBuffer))
        {
            if (!InMemoryBufferState.EXCEEDED_LIMIT.equals(this.oldState))
            {
                this.oldState = InMemoryBufferState.EXCEEDED_LIMIT;
                this.eventDispatcher.update(new InMemoryBufferStateUpdate(this.drumName, this.bucketId,
                                                                          InMemoryBufferState.EXCEEDED_LIMIT));
            }
        }
        else
        {
            if (!InMemoryBufferState.WITHIN_LIMIT.equals(this.oldState))
            {
                this.oldState = InMemoryBufferState.WITHIN_LIMIT;
                this.eventDispatcher.update(new InMemoryBufferStateUpdate(this.drumName, this.bucketId,
                                                                          InMemoryBufferState.WITHIN_LIMIT));
            }
        }

        try
        {
            this.lock.lock();
            this.isDataAvailable = true;
            this.dataAvailable.signal();
        }
        finally
        {
            this.lock.unlock();
        }
    }

    @Override
    public List<T> takeAll() throws InterruptedException
    {
        if (!this.isDataAvailable && this.stopRequested)
        {
            LOG.trace("[{}] - [{}] - stopped sending data!", this.drumName, this.bucketId);
            return null;
        }

        try
        {
            this.lock.lock();

            // check if data is already available or we have to wait
            if (!this.isDataAvailable)
            {
                this.dataAvailable.await(); // wait for available data
            }

            ConcurrentLinkedQueue<T> queue = this.flip();
            List<T> ret;
            if (queue.isEmpty())
            {
                ret = Collections.emptyList();
            }
            else
            {
                ret = new ArrayList<>(queue);
            }
            this.isDataAvailable = false;
            LOG.debug("[{}] - [{}] - transmitting data objects", this.drumName, this.bucketId);
            if (LOG.isTraceEnabled())
            {
                ret.forEach(entry -> LOG.trace("Transmitted: ", entry));
            }

            // clear the old "copied" content
            queue.clear();

            // return the copy
            return ret;
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
            this.dataAvailable.signal();
        }
        finally
        {
            this.lock.unlock();
        }
    }

    /**
     * Flips the currently active buffer with the back-buffer and notifies other threads about the availability of data
     *
     * @return Returns a reference to the currently inactive buffer
     */
    private ConcurrentLinkedQueue<T> flip()
    {
        int active = this.activeBuffer.get();
        int inactive = active ^ 1;
        LOG.debug("[{}] - [{}] - [{}->{}] - flipping buffers", this.drumName, this.bucketId, active, inactive);
        // the currently inactive buffer still hold the bytes of the period
        // when the buffer was last active, so clear it first
        MemoryBuffer<T> inactiveBuffer = this.buffers[inactive];
        inactiveBuffer.setByteLengthKV(0);
        inactiveBuffer.setByteLengthAux(0);
        // and now flip the currently active buffer
        this.activeBuffer.compareAndSet(active, inactive);

        this.eventDispatcher
                .update(new InMemoryBufferStateUpdate(this.drumName, this.bucketId, InMemoryBufferState.EMPTY));
        // the value of the active variable still points to the now inactive buffer so we are safe here
        return this.buffers[active].getBuffer();
    }
}
