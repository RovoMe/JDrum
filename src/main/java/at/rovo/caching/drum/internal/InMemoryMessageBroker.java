package at.rovo.caching.drum.internal;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import at.rovo.caching.drum.IBroker;
import at.rovo.caching.drum.data.ByteSerializer;
import at.rovo.caching.drum.event.DrumEventDispatcher;
import at.rovo.caching.drum.event.InMemoryBufferEvent;
import at.rovo.caching.drum.event.InMemoryBufferState;
import at.rovo.caching.drum.event.InMemoryBufferStateUpdate;

/**
 * <p>
 * <em>InMemoryMessageBroker</em> is a {@link IBroker} implementation
 * which manages {@link InMemoryData} objects.
 * </p>
 * <p>
 * This implementation buffers data into an active buffer while an attached 
 * consumer (IDiskWriter) instance is writing the data stored in the 
 * back-buffer.
 * </p>
 * <p>
 * A value representing the admissible byte size can be provided which sends an
 * event if the bytes stored in the active buffer exceeds this value. The value 
 * of the byte size can be set on invoking the constructor.
 * </p>
 * 
 * @param <T>
 *            The type of the data the broker manages
 * @param <V>
 *            The type of the value
 * @param <A>
 *            The type of the auxiliary data attached to a key
 * 
 * @author Roman Vottner
 */
public class InMemoryMessageBroker<T extends InMemoryData<V, A>, 
		V extends ByteSerializer<V>, A extends ByteSerializer<A>>
		implements IBroker<T, V, A>
{
	/** The logger of this class **/
	private final static Logger LOG =
			LogManager.getLogger(InMemoryMessageBroker.class);

	/** The name of the DRUM instance **/
	private String drumName = null;
	/**
	 * The object responsible for updating listeners on state or statistic
	 * changes
	 **/
	private DrumEventDispatcher eventDispatcher = null;
	/** The ID of the buffer **/
	private int bucketId = 0;
	/** The buffer array. The activeBuffer index will point to the currently
	 * active buffer **/
	@SuppressWarnings("unchecked")
	private ConcurrentLinkedQueue<T>[] buffer =
			(ConcurrentLinkedQueue<T>[]) Array.newInstance(ConcurrentLinkedQueue.class, 2);
	/** The index of the currently active buffer **/
	private AtomicBoolean activeBuffer = new AtomicBoolean(false);
	/** Will contain the the index of the buffers as key and the size of each
	 * key/value buffer as value **/
	private ConcurrentHashMap<Boolean, Integer> byteLengthKV = new ConcurrentHashMap<>(2);
	/** Contains the index of the buffers as key and the size of each auxiliary
	 * buffer as value **/
	private ConcurrentHashMap<Boolean, Integer> byteLengthAux = new ConcurrentHashMap<>(2);

	/**
	 * The size of the buffer before the two buffers get exchanged and the
	 * results being available through <code>takeAll</code>
	 **/
	private int byteSizePerBuffer = 0;

	/**
	 * Indicates if the thread the runnable part is running in should stop its
	 * work
	 **/
	private volatile boolean stopRequested = false;
	/**
	 * The old state of the crawler. Used to minimize state updates if the state
	 * remained the same as the old state
	 **/
	private InMemoryBufferState oldState = null;

	/** The lock object used to synchronize the threads **/
	private Lock lock = new ReentrantLock();
	/** Informs a waiting thread, which invoked await() previously, that data is 
	 * available for writing to disk bucket through invoking signal(). **/
	private Condition dataAvailable = lock.newCondition();
	
	/** Flag to indicate that the data is available **/
	private volatile boolean isDataAvailable = false;
		
	/**
	 * <p>
	 * Creates a new instance and initializes necessary fields.
	 * </p>
	 * 
	 * @param byteSizePerBuffer
	 *            The length of the buffer in size upon which the consumer will
	 *            get the buffered data
	 */
	public InMemoryMessageBroker(String drumName, int id,
			int byteSizePerBuffer, DrumEventDispatcher eventDispatcher)
	{
		this.drumName = drumName;
		this.eventDispatcher = eventDispatcher;
		this.bucketId = id;
		this.byteSizePerBuffer = byteSizePerBuffer;

		this.buffer[0] = new ConcurrentLinkedQueue<>();
		this.buffer[1] = new ConcurrentLinkedQueue<>();

		// set the initial key/value size of each buffer to 0
		this.byteLengthKV.put(false, 0);
		this.byteLengthKV.put(true, 0);

		// set the initial auxiliary size of each buffer to 0
		this.byteLengthAux.put(false, 0);
		this.byteLengthAux.put(true, 0);

		// the old state used to filter multiple state updates on the same state
		this.oldState = InMemoryBufferState.EMPTY;
		this.eventDispatcher.update(new InMemoryBufferStateUpdate(
				this.drumName, this.bucketId, InMemoryBufferState.EMPTY));
	}


	@Override
	public void put(T data)
	{
		LOG.info("[{}] - [{}] - Received data-object: {}; value: {}; aux: {} for operation: {}",
				this.drumName, this.bucketId, data.getKey(), data.getValue(), 
				data.getAuxiliary(), data.getOperation());

		boolean activeQueue = this.activeBuffer.get();
		this.buffer[this.bool2Int(activeQueue)].add(data);

		Integer keyLength = data.getKeyAsBytes().length;
		Integer valLength = 0;
		if (data.getValue() != null)
			valLength = data.getValueAsBytes().length;
		int auxLength = 0;
		if (data.getAuxiliary() != null)
			auxLength = data.getAuxiliaryAsBytes().length;

		int bytesKV = this.byteLengthKV.get(activeQueue) + (keyLength + valLength);
		this.byteLengthKV.put(activeQueue, bytesKV);
		int bytesAux = this.byteLengthAux.get(activeQueue) + auxLength;
		this.byteLengthAux.put(activeQueue, bytesAux);

		this.eventDispatcher.update(new InMemoryBufferEvent(this.drumName,
				this.bucketId, bytesKV, bytesAux));
		
		if ((this.byteLengthKV.get(activeQueue) > this.byteSizePerBuffer
				|| this.byteLengthAux.get(activeQueue) > this.byteSizePerBuffer))
		{
			if (!InMemoryBufferState.EXCEEDED_LIMIT.equals(this.oldState))
			{
				this.oldState = InMemoryBufferState.EXCEEDED_LIMIT;
				this.eventDispatcher.update(new InMemoryBufferStateUpdate(
						this.drumName, this.bucketId,
						InMemoryBufferState.EXCEEDED_LIMIT));
			}
		}
		else
		{
			if (!InMemoryBufferState.WITHIN_LIMIT.equals(this.oldState))
			{
				this.oldState = InMemoryBufferState.WITHIN_LIMIT;
				this.eventDispatcher.update(new InMemoryBufferStateUpdate(
						this.drumName, this.bucketId,
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

			this.flip();

			// make a copy of the list
			boolean active = this.activeBuffer.get();
			// the data to send is in the currently inactive buffer after the flip
			ConcurrentLinkedQueue<T> queue = this.buffer[this.bool2Int(!active)];
			List<T> ret;
			if (queue.isEmpty())
				ret = Collections.emptyList();
			else
				ret = new ArrayList<>(queue);
			this.isDataAvailable = false;
			LOG.debug("[{}] - [{}] - transmitting data objects", this.drumName, this.bucketId);

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
	 * <p>
	 * Flips the currently active buffer with the back-buffer and notifies other
	 * threads about the availability of data
	 * </p>
	 */
	private void flip()
	{
		if (LOG.isDebugEnabled())
			LOG.debug("[{}] - [{}] - flipping buffers", this.drumName, this.bucketId);
		boolean active = this.activeBuffer.get();
		// the currently inactive buffer still hold the bytes of the period
		// when the buffer was last active, so clear it first
		this.byteLengthKV.put(!active, 0);
		this.byteLengthAux.put(!active, 0);
		// and now flip the currently active buffer
		this.activeBuffer.compareAndSet(active, !active);

		this.eventDispatcher.update(new InMemoryBufferStateUpdate(
				this.drumName, this.bucketId, InMemoryBufferState.EMPTY));
	}

	/**
	 * <p>
	 * Converts a boolean to an integer value where a value of true will be
	 * converted to 1 and a value of false will return 0.
	 * </p>
	 *
	 * @param val The boolean value to convert to an int
	 * @return 1 if the value is true; 0 otherwise
	 */
	private int bool2Int(boolean val)
	{
		return (val) ? 1 : 0;
	}
}
