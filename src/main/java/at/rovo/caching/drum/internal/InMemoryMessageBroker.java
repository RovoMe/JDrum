package at.rovo.caching.drum.internal;

import java.util.ArrayList;
import java.util.List;
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
import at.rovo.caching.drum.util.ByteLength;

/**
 * <p>
 * <em>InMemoryMessageBroker</em> is a {@link IBroker} implementation
 * which manages {@link InMemoryData} objects.
 * </p>
 * <p>
 * This implementation buffers data up to a predefined byte size before it
 * allows further processing of data. The value of the byte size can be set on
 * invoking the constructor.
 * </p>
 * <p>
 * On exceeding the byte size the internal buffer is switched with a second
 * empty buffer to enable message processing while avoiding synchronization
 * locks.
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
	private final static Logger logger = LogManager.getLogger(InMemoryMessageBroker.class);

	/** The name of the DRUM instance **/
	private String drumName = null;
	/**
	 * The object responsible for updating listeners on state or statistic
	 * changes
	 **/
	private DrumEventDispatcher eventDispatcher = null;
	/** The ID of the buffer **/
	private int bucketId = 0;
	/** The list receiving data elements at the start **/
	private List<T> buffer1 = null;
	/**
	 * The backup list to receive new data elements when the first buffer is
	 * processed
	 **/
	private List<T> buffer2 = null;
	/** The length of the key/value data in both buffers **/
	private ByteLength<T> byteLengthKV = null;
	/** The length of the auxiliary data in both buffers **/
	private ByteLength<T> byteLengthAux = null;
	/** The currently active buffer which is filled with data **/
	private List<T> activeQueue = null;
	/**
	 * The size of the buffer before the two buffers get exchanged and the
	 * results being available through <code>takeAll</code>
	 **/
	private int byteSizePerBuffer = 0;
	/**
	 * A reference to the currently not active buffer. This buffer is currently
	 * processed
	 **/
	private List<T> backBuffer = null;
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

		this.buffer1 = new ArrayList<T>();
		this.buffer2 = new ArrayList<T>();

		this.byteLengthKV = new ByteLength<T>();
		this.byteLengthKV.set(this.buffer1, 0);
		this.byteLengthKV.set(this.buffer2, 0);

		this.byteLengthAux = new ByteLength<T>();
		this.byteLengthAux.set(this.buffer1, 0);
		this.byteLengthAux.set(this.buffer2, 0);

		this.activeQueue = this.buffer1;
		this.backBuffer = this.buffer2;

		// the old state used to filter multiple state updates on the same state
		this.oldState = InMemoryBufferState.EMPTY;
		this.eventDispatcher.update(new InMemoryBufferStateUpdate(
				this.drumName, this.bucketId, InMemoryBufferState.EMPTY));
	}

	@Override
	public void put(T data)
	{
		logger.info("[{}] - [{}] - Received data-object: {}; value: {}; aux: {} for operation: {}", 
				this.drumName, this.bucketId, data.getKey(), data.getValue(), 
				data.getAuxiliary(), data.getOperation());

		this.activeQueue.add(data);

		Integer keyLength = data.getKeyAsBytes().length;
		Integer valLength = 0;
		if (data.getValue() != null)
			valLength = data.getValueAsBytes().length;
		int auxLength = 0;
		if (data.getAuxiliary() != null)
			auxLength = data.getAuxiliaryAsBytes().length;

		int bytesKV = this.byteLengthKV.get(this.activeQueue) + (keyLength + valLength);
		this.byteLengthKV.set(this.activeQueue, bytesKV);
		int bytesAux = this.byteLengthAux.get(this.activeQueue) + auxLength;
		this.byteLengthAux.set(this.activeQueue, bytesAux);

		this.eventDispatcher.update(new InMemoryBufferEvent(this.drumName,
				this.bucketId, bytesKV, bytesAux));
		
		if ((this.byteLengthKV.get(this.activeQueue) > this.byteSizePerBuffer 
				|| this.byteLengthAux.get(this.activeQueue) > this.byteSizePerBuffer))
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
		if (this.stopRequested)
		{
			logger.trace("[{}] - [{}] - stopped sending data!", this.drumName, this.bucketId);
			return null;
		}
			
		try
		{
			this.lock.lock();
			
			// check if data is already available or we have to wait
			if (!this.isDataAvailable)
				this.dataAvailable.await(); // wait for available data

			this.flip();
			
			// make a copy of the list
			List<T> ret = new ArrayList<T>(this.backBuffer);
			this.isDataAvailable = false;
			logger.debug("[{}] - [{}] - transmitting data objects", this.drumName, this.bucketId);
			
			// clear the old "written" content
			this.backBuffer.clear();
	
			// return the copy
			return ret;
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
		if (logger.isDebugEnabled())
			logger.debug("[{}] - [{}] - flipping buffers", this.drumName, this.bucketId);
		if (this.buffer1.equals(this.activeQueue))
		{
			this.activeQueue = this.buffer2;
			this.backBuffer = this.buffer1;
		}
		else
		{
			this.activeQueue = this.buffer1;
			this.backBuffer = this.buffer2;
		}

		this.byteLengthKV.set(this.activeQueue, 0);
		this.byteLengthAux.set(this.activeQueue, 0);

		this.eventDispatcher.update(new InMemoryBufferStateUpdate(
				this.drumName, this.bucketId, InMemoryBufferState.EMPTY));
	}

	@Override
	public void stop()
	{
		logger.trace("[{}] - [{}] - stop requested!", this.drumName, this.bucketId);
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
}
