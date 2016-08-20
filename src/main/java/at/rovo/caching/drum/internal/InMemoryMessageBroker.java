package at.rovo.caching.drum.internal;

import at.rovo.caching.drum.Broker;
import at.rovo.caching.drum.DrumEventDispatcher;
import at.rovo.caching.drum.event.InMemoryBufferEvent;
import at.rovo.caching.drum.event.InMemoryBufferState;
import at.rovo.caching.drum.event.InMemoryBufferStateUpdate;
import at.rovo.caching.drum.util.lockfree.FlippableData;
import at.rovo.caching.drum.util.lockfree.FlippableDataContainer;
import at.rovo.common.annotations.GuardedBy;
import at.rovo.common.annotations.ThreadSafe;
import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * <em>InMemoryMessageBroker</em> is a {@link Broker} implementation which manages {@link InMemoryEntry} objects. It
 * will store new data objects in a lock free {@link FlippableDataContainer} on invoking {@link #put(InMemoryEntry)} and
 * return all currently buffered data objects through invoking {@link #takeAll()}. This implementation will block
 * consumer threads if no buffered data are currently available. If a {@link #stop} was requested, {@link #takeAll()}
 * semantics changes and instead of blocking it will return data immediately in order to push data to consumers as fast
 * as possible.
 * <p>
 * On invoking {@link #takeAll()} the backing {@link FlippableDataContainer} will be flipped which results in the buffer
 * holding the buffered data from being returned while a new {@link Queue} is set to store new received {@link
 * InMemoryEntry} objects. The flip will be executed atomically guaranteeing that no data is lost while processing the
 * flip operation.
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
@ThreadSafe
public class InMemoryMessageBroker<T extends InMemoryEntry<V, A>, V extends Serializable, A extends Serializable>
        implements Broker<T, V>
{
    /** The logger of this class **/
    private final static Logger LOG = LogManager.getLogger(MethodHandles.lookup().lookupClass());

    // final state
    /** The name of the DRUM instance **/
    private final String drumName;
    /** The object responsible for updating listeners on state or statistic changes **/
    private final DrumEventDispatcher eventDispatcher;
    /** The ID of the buffer **/
    private final int bucketId;
    /** The size of the buffer before the two buffers get exchanged and the results being available through
     * <code>takeAll</code> **/
    private final int byteSizePerBuffer;

    /** The lock object is needed to let consumers wait on invoking {@link #takeAll()} if no data is available **/
    private final Lock lock = new ReentrantLock();
    /** Informs a waiting thread, which invoked await() previously, that data is available for writing to disk bucket
     * through invoking signal() **/
    private final Condition dataAvailable = lock.newCondition();

    // modifiable state
    /** The old state of the crawler. Used to minimize state updates if the state remained the same as the old state **/
    private InMemoryBufferState oldState = null;
    /** Indicates if the thread the runnable part is running in should stop its work **/
    private volatile boolean isStopRequested = false;
    /** To avoid logging of multiple stopped sending data messages **/
    private boolean stopAlreadyLogged = false;
    /** A reference to the thread which executes the <code>takeAll()</code> logic in order to interrupt a blocking wait
     * for further data on application shutdown **/
    private Thread consumerThread = null;

    /** The flippable lock-free buffer to add in memory data to **/
    @GuardedBy("lock")
    private FlippableDataContainer<T> buffer = new FlippableDataContainer<>();

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
        this.oldState = updateState(InMemoryBufferState.EMPTY);
    }

    @Override
    public void stop()
    {
        LOG.trace("[{}] - [{}] - stop requested!", this.drumName, this.bucketId);
        this.isStopRequested = true;

        updateState(InMemoryBufferState.STOPPED);

        if (null != this.consumerThread)
        {
            this.consumerThread.interrupt();
        }
    }

    /**
     * Generates a new event for the provided {@link InMemoryBufferState state}.
     *
     * @param newState
     *         The new {@link InMemoryBufferState} to set
     *
     * @return Returns a reference of the new state
     */
    private InMemoryBufferState updateState(InMemoryBufferState newState)
    {
        this.eventDispatcher.update(new InMemoryBufferStateUpdate(this.drumName, this.bucketId, newState));
        return newState;
    }

    /**
     * Generates a new {@link InMemoryBufferEvent event} for the given <em>byteLengthKV</em> and <em>byteLengthAux</em>
     * values.
     *
     * @param byteLengthKV
     *         The length of the key-value pair bytes
     * @param byteLengthAux
     *         The length of the auxiliary data bytes
     */
    private void updateState(int byteLengthKV, int byteLengthAux)
    {
        this.eventDispatcher.update(new InMemoryBufferEvent(this.drumName, this.bucketId, byteLengthKV, byteLengthAux));
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    ///                                invoked usually by producer threads                                           ///
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    @Override
    public void put(T data)
    {
        if (this.isStopRequested)
        {
            throw new IllegalStateException("Could not accept further data entries as a stop was already requested");
        }
        if (null == data)
        {
            return;
        }

        FlippableData<T> _data = this.buffer.put(data);

        LOG.info("[{}] - [{}] - Received data-object: {}; value: {}; aux: {} for operation: {}", this.drumName,
                 this.bucketId, data.getKey(), data.getValue(), data.getAuxiliary(), data.getOperation());

        int byteLengthKV = _data.getKeyLength() + _data.getValLength();
        int byteLengthAux = _data.getAuxLength();

        updateState(byteLengthKV, byteLengthAux);

        this.checkStateChange(byteLengthKV, byteLengthAux);

        this.lock.lock();
        try
        {
            // signal that data is available. This should wake up a thread waiting on data
            this.dataAvailable.signal();
        }
        finally
        {
            this.lock.unlock();
        }
    }

    /**
     * Checks if the provided byte length of the key-value or auxiliary data exceed a predefined threshold value and if
     * so will trigger a state change which indicates that the limit was exceeded. In order to avoid multiple
     * notifications on the same state change, this implementation includes a check with the previous state and only
     * fires an event if the previous state does not equal the new state and thus indicate a real state change.
     *
     * @param byteLengthKV
     *         The length of the key and value bytes
     * @param byteLengthAux
     *         The length of the auxiliary data bytes
     */
    private void checkStateChange(int byteLengthKV, int byteLengthAux)
    {
        if ((byteLengthKV > this.byteSizePerBuffer ||
             byteLengthAux > this.byteSizePerBuffer))
        {
            if (!InMemoryBufferState.EXCEEDED_LIMIT.equals(this.oldState))
            {
                this.oldState = updateState(InMemoryBufferState.EXCEEDED_LIMIT);
            }
        }
        else
        {
            if (!InMemoryBufferState.WITHIN_LIMIT.equals(this.oldState))
            {
                this.oldState = updateState(InMemoryBufferState.WITHIN_LIMIT);
            }
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    ///                                invoked usually by consumer threads                                           ///
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    /**
     * Will return all currently available items from the buffer on atomically flipping the buffer.
     * <p>
     * As thread interruption can occur anytime without any user-based trigger, this method maintains a rather passive
     * handling of {@link InterruptedException} by catching it internally, which will also remove the interrupted flag
     * from the thread that invoked this method. This guarantees that the currently available data is still returned to
     * the invoking instance so that it has a chance to process the data further. This however can lead to a consecutive
     * invocation after the interrupt was noticed, which will therefore block again.
     * <p>
     * If {@link #stop} was requested previous to invoking this method, this method will keep returning data until the
     * internal buffer is emptied. Once a stop was requested, this method will no longer block. If no data is available
     * any further, this method will return an empty queue.
     *
     * @return While not stopped, this method will return a {@link Queue} containing the data which was available on
     * buffer flip time. If an interruption was noticed while the consumer thread was waiting on new data, a queue
     * without any data might get returned. After a stop was requested via {@link #stop()} this method keeps returning
     * data as long as available. Once no data is available any further, an empty {@link Queue} is returned.
     */
    @Override
    public Queue<T> takeAll()
    {

        if (this.buffer.isEmpty() && this.isStopRequested)
        {
            if (!this.stopAlreadyLogged)
            {
                LOG.trace("[{}] - [{}] - stopped sending data!", this.drumName, this.bucketId);
                this.stopAlreadyLogged = true;
            }
            return this.buffer.flip();
        }

        // needed if acquiring the lock threw the exception as there was no lock acquired then and as a consequence an
        // IllegalMonitorException is thrown on releasing the lock
        boolean lockAcquired = false;
        try
        {
            this.lock.lockInterruptibly();
            lockAcquired = true;
            // safe the reference to the current thread in order to interrupt the blocking await in case of an
            // application shutdown
            this.consumerThread = Thread.currentThread();
            LOG.trace("[{}] - [{}] - Acquired lock of buffer", this.drumName, this.bucketId);
            while (this.buffer.isEmpty() && !this.isStopRequested)
            {
                // wait till data is available if not a stop was requested
                this.dataAvailable.await();
            }
        }
        catch (InterruptedException iEx)
        {
            LOG.debug("[{}] - [{}] - Interrupted while waiting on in-memory data", this.drumName, this.bucketId);
        }
        finally
        {
            LOG.trace("[{}] - [{}] - Releasing lock of buffer", this.drumName, this.bucketId);
            if (lockAcquired)
            {
                this.lock.unlock();
            }
        }

        final Queue<T> queue = this.buffer.flip();
        LOG.debug("[{}] - [{}] - Flipped buffers. Transmitting {} data objects",
                  this.drumName, this.bucketId, queue.size());

        if (LOG.isTraceEnabled())
        {
            Queue<T> copy = new ConcurrentLinkedQueue<>(queue);
//            copy.forEach(entry -> LOG.trace("[{}] - [{}] - Transmitted: {}", this.drumName, this.bucketId, entry));
            for (T entry : copy) {
                LOG.trace("[{}] - [{}] - Size: {} - Transmitted: {}", this.drumName, this.bucketId, copy.size(), entry);
            }
        }

        return queue;
    }
}
