package at.rovo.caching.drum;

import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import at.rovo.caching.drum.event.DrumEventDispatcher;
import at.rovo.caching.drum.event.InMemoryBufferEvent;
import at.rovo.caching.drum.event.InMemoryBufferState;
import at.rovo.caching.drum.event.InMemoryBufferStateUpdate;

/**
 * <p><code>InMemoryMessageBroker</code> is a {@link IBroker} implementation
 * which manages {@link InMemoryData} objects.</p>
 * <p>This implementation buffers data up to a predefined byte size before it
 * allows further processing of data. The value of the byte size can be set on
 * invoking the constructor.</p>
 * <p>On exceeding the byte size the internal buffer is switched with a second
 * empty buffer to enable message processing while avoiding synchronization 
 * locks.</p>
 *
 * @param <T> The type of the data the broker manages
 * 
 * @author Roman Vottner
 */
public class InMemoryMessageBroker<T extends InMemoryData<V,A>, 
	V extends ByteSerializer<V>, A extends ByteSerializer<A>> implements IBroker<T,V,A>
{
	/** The logger of this class **/
	private final static Logger logger = LogManager.getLogger(InMemoryMessageBroker.class);
	
	/** The name of the DRUM instance **/
	private String drumName = null;
	/** The object responsible for updating listeners on state or statistic 
	 * changes **/
	private DrumEventDispatcher eventDispatcher = null; 
	/** The ID of the buffer**/
	private int bucketId = 0;
	/** The list receiving data elements at the start **/
	private List<T> buffer1 = null;
	/** The backup list to receive new data elements when the first buffer is 
	 * processed **/
	private List<T> buffer2 = null;
	/** The length of the key/value data in the both buffers **/
	private ByteLength<T> byteLengthKV = null;
	/** The length of the auxiliary data in the both buffers **/
	private ByteLength<T> byteLengthAux = null;
	/** The currently active buffer which is filled with data **/
	private List<T> activeQueue = null;
	/** The size of the buffer before the two buffers get exchanged and the 
	 * results being available through <code>takeAll</code>**/
	private int byteSizePerBuffer = 0;
	/** A reference to the currently not active buffer. This buffer is currently
	 * processed **/
	private List<T> backBuffer = null;
	/** Indicates if the thread the runnable part is running in should stop its
	 * work **/
	private volatile boolean stopRequested = false;
	/** Flag to indicate if data is available to send to the disk writer **/
	private boolean dataAvailable = false;
	/** Flag to indicate that a user forced synchronization is in progress **/
	private boolean synchInProgress = false;
	/** The old state of the crawler. Used to minimize state updates if the state
	 * remained the same as the old state **/
	private InMemoryBufferState oldState = null;
	/** Invokes {@link #invokeDispatch()} every few milliseconds to tests if 
	 * enough data are available and a disk writer is waiting for data **/
	private DiskWriterDispatchInvoker<T,V,A> dispatchInvoker = null;
	/** The actual thread executing the dispatch invoker **/
	private Thread dispatchInvokerThread = null;
		
	/**
	 * <p>Creates a new instance and initializes necessary fields.</p>
	 * 
	 * @param byteSizePerBuffer The length of the buffer in size upon which the
	 *                          consumer will get the buffered data
	 */
	public InMemoryMessageBroker(String drumName, int id, int byteSizePerBuffer, DrumEventDispatcher eventDispatcher)
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
		this.eventDispatcher.update(new InMemoryBufferStateUpdate(this.drumName, this.bucketId, InMemoryBufferState.EMPTY));
		
		// A backing thread which will notify the waiting disk writer in case
		// enough data are available
		this.dispatchInvoker = new DiskWriterDispatchInvoker<T,V,A>(this);
		this.dispatchInvokerThread = new Thread(this.dispatchInvoker);
		this.dispatchInvokerThread.setName(this.drumName+"-Writer-DisptachInvoker-"+this.bucketId);
		this.dispatchInvokerThread.setPriority(Math.min(10,this.dispatchInvokerThread.getPriority()+1));
		this.dispatchInvokerThread.start();
	}
	
	@Override
	public synchronized void put(T data)
	{
		if (logger.isInfoEnabled())
			logger.info("["+this.drumName+"] - ["+this.bucketId+"] - Received data-object: "+data.getKey()+"; value: "+data.getValue()+"; aux: "+data.getAuxiliary()+" for operation: "+data.getOperation());
		
		this.activeQueue.add(data);
		
		Integer keyLength = data.getKeyAsBytes().length;
		Integer valLength = 0;
		if (data.getValue() != null)
			valLength = data.getValueAsBytes().length;
		int auxLength = 0;
		if (data.getAuxiliary() != null)
			auxLength = data.getAuxiliaryAsBytes().length;
		
		int bytesKV = this.byteLengthKV.get(this.activeQueue) + (keyLength+valLength);
		this.byteLengthKV.set(this.activeQueue, bytesKV);
		int bytesAux = this.byteLengthAux.get(this.activeQueue) + auxLength;
		this.byteLengthAux.set(this.activeQueue, bytesAux);
		
		this.eventDispatcher.update(new InMemoryBufferEvent(this.drumName, this.bucketId, bytesKV, bytesAux));
		
		if ((this.byteLengthKV.get(this.activeQueue) > this.byteSizePerBuffer || 
				this.byteLengthAux.get(this.activeQueue) > this.byteSizePerBuffer))
		{
			this.dataAvailable = true;
			if (!this.dispatchInvoker.isPolling())
				this.dispatchInvoker.startPolling();
			
			if (!InMemoryBufferState.EXCEEDED_LIMIT.equals(this.oldState))
			{
				this.oldState = InMemoryBufferState.EXCEEDED_LIMIT;
				this.eventDispatcher.update(new InMemoryBufferStateUpdate(this.drumName, this.bucketId, InMemoryBufferState.EXCEEDED_LIMIT));
			}
		}
		else 
		{
			this.dataAvailable = false;
			this.dispatchInvoker.startPolling();
			
			if (!InMemoryBufferState.WITHIN_LIMIT.equals(this.oldState))
			{
				this.oldState = InMemoryBufferState.WITHIN_LIMIT;
				this.eventDispatcher.update(new InMemoryBufferStateUpdate(this.drumName, this.bucketId, InMemoryBufferState.WITHIN_LIMIT));
			}
		}
	}

	@Override
	public synchronized List<T> takeAll() throws InterruptedException
	{
		if (this.stopRequested)
			return null;
			
		if (!this.dataAvailable)
		{
			this.dispatchInvoker.startPolling();
		}
		
		// takeAll is invoked by an other thread, to block until data
		// is available we need to wait - we get notified 
		this.wait();
		
		this.dispatchInvoker.stopPolling();
		
		this.flip();
			
		if (logger.isDebugEnabled())
			logger.debug("["+this.drumName+"] - ["+this.bucketId+"] - transmitting data objects");
		// make a copy of the list
		List<T> ret = new ArrayList<T>(this.backBuffer);
		// clear the old "written" content
		this.backBuffer.clear();
			
		// return the copy
		return ret;
	}
	
	/**
	 * <p>Flips the currently active buffer with the back-buffer and notifies 
	 * other threads about the availability of data</p>
	 */
	private void flip()
	{
		if (logger.isDebugEnabled())
			logger.debug("["+this.drumName+"] - ["+this.bucketId+"] - flipping buffers");
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
		
		this.byteLengthKV.set(this.activeQueue,  0);
		this.byteLengthAux.set(this.activeQueue,  0);
		
		this.eventDispatcher.update(new InMemoryBufferStateUpdate(this.drumName, this.bucketId, InMemoryBufferState.EMPTY));
		
		this.dataAvailable = true;	
	}

	@Override
	public List<T> flush()
	{
		List<T> copy = null;
		synchronized (this)
		{
		this.synchInProgress  = true;
		if (logger.isDebugEnabled())
			logger.debug("["+this.drumName+"] - ["+this.bucketId+"] - flushing buffers");
		
		// put, flip and flush are all executed within the same thread so no
		// need to synchronize them
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
		
		this.byteLengthKV.set(this.activeQueue,  0);
		this.byteLengthAux.set(this.activeQueue,  0);
		
		// if there are elements in the active queue copy them to the back buffer
		for (T data : this.activeQueue)
		{
			if (!this.backBuffer.contains(data))
				this.backBuffer.add(data);
		}
		this.activeQueue.clear();	
		
		this.oldState = InMemoryBufferState.EMPTY;
		this.eventDispatcher.update(new InMemoryBufferStateUpdate(this.drumName, this.bucketId, InMemoryBufferState.EMPTY));
	
		// create a copy of the data and clear the original list to prevent
		// the same entry to appear in multiple writes
		copy = new ArrayList<>(this.backBuffer);
		this.backBuffer.clear();
		}
		
		this.synchInProgress = false;
		
		return copy;
	}

	@Override
	public void stop()
	{
		// no more data to expect - send the rest to the consumer
		this.flip();		
		this.stopRequested = true;
		
		if (this.dispatchInvoker != null)
			this.dispatchInvoker.stop();
		
		if (this.dispatchInvokerThread != null && 
				this.dispatchInvokerThread.isAlive())
		{
			try
			{
				if (this.dispatchInvokerThread.getState().equals(Thread.State.WAITING))
					this.dispatchInvokerThread.interrupt();
				this.dispatchInvokerThread.join();
			}
			catch (InterruptedException e)
			{
				e.printStackTrace();
			}
		}
		
		synchronized (this)
		{
			this.notify();
		}
	}
	
	synchronized void invokeDispatch()
	{
		if (!this.synchInProgress)
		{
			if (this.dataAvailable)
			{					
				// notify waiting elements that data is available
				this.notify();
			}
		}
	}
}
