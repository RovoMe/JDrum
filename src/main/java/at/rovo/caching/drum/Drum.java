package at.rovo.caching.drum;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import at.rovo.caching.drum.event.DrumEventDispatcher;
import at.rovo.caching.drum.event.DrumSynchronizeEvent;

/**
 * <p>This implementation of the 'Disk Repository with Update Management' 
 * structure utilizes a consumer/producer pattern to store and process input
 * received by its</p>
 * <ul>
 * <li>{@link #check(Number)} or {@link #check(Number, ByteSerializable)}</li>
 * <li>{@link #update(Number, ByteSerializable)} or {@link #update(Number, 
 * ByteSerializable, ByteSerializable)}</li>
 * <li>{@link #checkUpdate(Number, ByteSerializable) or {@link #checkUpdate(Number, 
 * ByteSerializable, ByteSerializable)}}</li>
 * </ul>
 * <p>methods.</p>
 * <p>Internally <code>numBuckets</code> buffers and buckets will be created 
 * which will hold the data sent to DRUM. Buffers are the in-memory storage
 * while buckets are the intermediary disk files. Buffers fill up to <code>
 * bufferSize</code> bytes before they get sent to a disk file.</p>
 * 
 *
 * @param <V> The type of the value
 * @param <A> The type of the auxiliary data attached to a key
 * 
 * @author Roman Vottner
 */
public class Drum<V extends ByteSerializer<V>, A extends ByteSerializer<A>> 
	implements IDrum<V, A>
{
	private final static Logger logger = LogManager.getLogger(Drum.class);
	
	/** The name of the DRUM instance **/
	protected String drumName = null;
	/** The number of buffers and buckets used **/
	protected int numBuckets = 0;
	/** The size of an in-memory buffer **/
	protected int bufferSize = 0;
	/** The broker list which holds elements in memory until they get written
	 * to the disk file **/
	protected List<IBroker<InMemoryData<V,A>,V,A>> inMemoryBuffer = null;
	/** The set of writer objects that listens to notifications of a broker
	 * and write content from the broker to a disk file **/
	protected List<IDiskWriter<V,A>> diskWriters = null;
	/** The object that compares keys of data-objects for their uniqueness and 
	 * merges them into the data store in case the need to be updated **/
	protected IMerger<V,A> merger = null;
	/** The execution service which hosts our threads **/
	protected ExecutorService executor = null;
	/** The merger thread **/
	protected Thread mergerThread = null;
	/** The event dispatcher used to inform listeners of internal state changes 
	 * and certain statistics**/
	protected DrumEventDispatcher eventDispatcher = new DrumEventDispatcher();
	/** The event dispatcher thread **/
	protected Thread eventDispatcherThread = null;
			
	/**
	 * <p>Creates a new instance and assigns initial values to attributes. The 
	 * DRUM instance will create a single CacheFile for storing the data</p>
	 * 
	 * @param drumName The name of the DRUM instance
	 * @param numBuckets The number of buckets to be used
	 * @param bufferSize The size of a single buffer in bytes
	 * @param dispatcher The {@link IDispatcher} implementation which will receive
	 *                   information on items added via <code>check</code>, 
	 *                   <code>update</code> or <code>checkUpdate</code>.
	 * @param valueClass The class-type of the value for a certain key
	 * @param auxClass The auxiliary data-type attached to a certain key
	 */
	public Drum(String drumName, int numBuckets, int bufferSize, 
			IDispatcher<V,A> dispatcher, Class<V> valueClass, Class<A> auxClass)
	{
		this.init(drumName, numBuckets, bufferSize, dispatcher, valueClass, auxClass, 
				DrumStorageFactory.getDefaultStorageFactory(drumName, numBuckets, dispatcher, valueClass, auxClass, this.eventDispatcher));
	}
	
	/**
	 * <p>Creates a new instance and assigns initial values to attributes. The 
	 * DRUM instance will create a single CacheFile for storing the data.</p>
	 * 
	 * @param drumName The name of the DRUM instance
	 * @param numBuckets The number of buckets to be used
	 * @param bufferSize The size of a single buffer in bytes
	 * @param dispatcher The {@link IDispatcher} implementation which will receive
	 *                   information on items added via <code>check</code>, 
	 *                   <code>update</code> or <code>checkUpdate</code>.
	 * @param valueClass The class-type of the value for a certain key
	 * @param auxClass The auxiliary data-type attached to a certain key
	 * @param listener The object which needs to be notified on certain internal
	 *                 state or statistic changes
	 */
	public Drum(String drumName, int numBuckets, int bufferSize, 
			IDispatcher<V,A> dispatcher, Class<V> valueClass, Class<A> auxClass, IDrumListener listener)
	{
		this.addDrumListener(listener);
		this.init(drumName, numBuckets, bufferSize, dispatcher, valueClass, auxClass, 
				DrumStorageFactory.getDefaultStorageFactory(drumName, numBuckets, dispatcher, valueClass, auxClass, this.eventDispatcher));
	}
	
	/**
	 * <p>Creates a new instance and assigns initial values to attributes. The 
	 * DRUM instance will create a data store depending on the provided storage 
	 * factory</p>
	 * 
	 * @param drumName The name of the DRUM instance
	 * @param numBuckets The number of buckets to be used
	 * @param bufferSize The size of a single buffer in bytes
	 * @param dispatcher The {@link IDispatcher} implementation which will receive
	 *                   information on items added via <code>check</code>, 
	 *                   <code>update</code> or <code>checkUpdate</code>.
	 * @param valueClass The class-type of the value for a certain key
	 * @param auxClass The auxiliary data-type attached to a certain key
	 * @param factory The factory object which defines where data should be stored
	 *                in. Note that factory must return an implementation of IMerger
	 */
	public Drum(String drumName, int numBuckets, int bufferSize, IDispatcher<V,A> dispatcher, 
			Class<V> valueClass, Class<A> auxClass, DrumStorageFactory<V,A> factory)
	{
		this.init(drumName, numBuckets, bufferSize, dispatcher, valueClass, auxClass, factory);
	}
	
	/**
	 * <p>Creates a new instance and assigns initial values to attributes. The 
	 * DRUM instance will create a data store depending on the provided storage 
	 * factory</p>
	 * 
	 * @param drumName The name of the DRUM instance
	 * @param numBuckets The number of buckets to be used
	 * @param bufferSize The size of a single buffer in bytes
	 * @param dispatcher The {@link IDispatcher} implementation which will receive
	 *                   information on items added via <code>check</code>, 
	 *                   <code>update</code> or <code>checkUpdate</code>.
	 * @param valueClass The class-type of the value for a certain key
	 * @param auxClass The auxiliary data-type attached to a certain key
	 * @param factory The factory object which defines where data should be stored
	 *                in. Note that factory must return an implementation of IMerger
	 * @param listener The object which needs to be notified on certain internal
	 *                 state or statistic changes
	 */
	public Drum(String drumName, int numBuckets, int bufferSize, IDispatcher<V,A> dispatcher, 
			Class<V> valueClass, Class<A> auxClass, DrumStorageFactory<V,A> factory,
			IDrumListener listener)
	{
		this.addDrumListener(listener);
		this.init(drumName, numBuckets, bufferSize, dispatcher, valueClass, auxClass, factory);
	}
	
	/**
	 * <p>Initializes the DRUM instance with required data and starts the worker 
	 * threads.</p>
	 * 
	 * @param drumName The name of the DRUM instance
	 * @param numBuckets The number of buckets to be used
	 * @param bufferSize The size of a single buffer in bytes
	 * @param dispatcher The {@link IDispatcher} implementation which will receive
	 *                   information on items added via <code>check</code>, 
	 *                   <code>update</code> or <code>checkUpdate</code>.
	 * @param valueClass The class-type of the value for a certain key
	 * @param auxClass The auxiliary data-type attached to a certain key
	 * @param factory The factory object which defines where data should be stored
	 *                in. Note that factory must return an implementation of IMerger
	 * @param listener The object which needs to be notified on certain internal
	 *                 state or statistic changes
	 */
	private void init(String drumName, int numBuckets, int bufferSize, IDispatcher<V,A> dispatcher, 
			Class<V> valueClass, Class<A> auxClass, DrumStorageFactory<V,A> factory)
	{
		this.eventDispatcherThread = new Thread(this.eventDispatcher);
		this.eventDispatcherThread.setName(drumName+"EventDispatcher");
		this.eventDispatcherThread.start();
		
		this.drumName = drumName;
		this.numBuckets = numBuckets;
		this.bufferSize = bufferSize;
		
		// create the broker and the consumer listening to the broker
		this.inMemoryBuffer = new 
				ArrayList<IBroker<InMemoryData<V,A>,V,A>>(numBuckets);
		this.diskWriters = new ArrayList<IDiskWriter<V,A>>(numBuckets);
		this.merger = factory.getStorage();
		
		DrumExceptionHandler exceptionHandler = new DrumExceptionHandler();
		NamedThreadFactory writerFactory = new NamedThreadFactory();
		writerFactory.setName(this.drumName+"-Writer");
		writerFactory.setUncaughtExceptionHanlder(exceptionHandler);
//		writerFactory.increasePriority(true);
//		this.executor = Executors.newCachedThreadPool(writerFactory);
		this.executor = Executors.newFixedThreadPool(this.numBuckets, writerFactory);
		
		for (int i=0; i<numBuckets; i++)
		{
			IBroker<InMemoryData<V,A>,V,A> broker = new InMemoryMessageBroker<InMemoryData<V,A>,V,A>(drumName, i, bufferSize, this.eventDispatcher);
			IDiskWriter<V,A> consumer = new DiskBucketWriter<V,A>(drumName, i, bufferSize, broker, this.merger, this.eventDispatcher);
			
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
		this.mergerThread = new Thread(this.merger, this.drumName+"-Merger");
//		this.mergerThread.setPriority(Math.min(10, this.mergerThread.getPriority()+1));
		this.mergerThread.setUncaughtExceptionHandler(exceptionHandler);
		this.mergerThread.start();	
		
//		Thread.currentThread().setPriority(Math.max(0, Thread.currentThread().getPriority()-1));
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
	public void synchronize() 
	{
		this.eventDispatcher.update(new DrumSynchronizeEvent(this.drumName));
		logger.info("["+this.drumName+"] - SYNCHRONISING");

		// send all currently buffered data to the disk writers to write these
		// into the bucket files
		List<List<InMemoryData<V,A>>> memoryData = new ArrayList<List<InMemoryData<V,A>>>();
		for (IBroker<InMemoryData<V,A>,V,A> broker : this.inMemoryBuffer)
			memoryData.add(broker.flush());
		
		int i = 0;
		for (IDiskWriter<V,A> writer : this.diskWriters)
		{
			writer.forceWrite(memoryData.get(i++));
		}
		
		// as we have used an in-thread invocation of the writer (the method is
		// executed in the main-threads context), writer has finished his task.
		
		// So we have actually to call the merger instance to tell him to merge
		// the data written to the respective bucket file as the bucket file
		// writer might not have had enough data to invoke the merger itself
		this.merger.forceMerge();
	}
	
	@Override
	public void dispose() 
	{	
		this.synchronize();
		
		// flip the buffers which sends the writers the latest data
		for (IBroker<?,?,?> broker : this.inMemoryBuffer)
			broker.stop();
		
		// give the threads a chance to finish their work without being
		// interrupted
		for (IDiskWriter<V,A> writer : this.diskWriters)
			writer.stop();

		this.executor.shutdown();
		
		// wait for the threads to finish
		try
		{
			this.executor.awaitTermination(1, TimeUnit.MINUTES);
		}
		catch (InterruptedException e)
		{
	
		}
		
		this.merger.stop();
		try
		{
			this.mergerThread.join();
		}
		catch (InterruptedException e)
		{
			e.printStackTrace();
		}
		
		// close the open resources held by the writers
		for (IDiskWriter<V,A> writer : this.diskWriters)
			writer.close();
		
		this.eventDispatcher.stop();
		this.eventDispatcherThread.interrupt();	
	}
	
	@Override
	public void addDrumListener(IDrumListener listener)
	{
		this.eventDispatcher.addDrumListener(listener);
	}
	
	@Override
	public void removeDrumListener(IDrumListener listener)
	{
		this.eventDispatcher.removeDrumListener(listener);
	}
	
	/**
	 * <p>Stores the key, the value and the auxiliary data as well as the 
	 * operation to be executed on these data in the according in-memory
	 * buffer.</p>
	 * 
	 * @param key The hash value of the data
	 * @param value The value associated with the key
	 * @param aux The auxiliary data of the key
	 * @param operation The operation to be used on the data
	 */
	private void add(Long key, V value, A aux, DrumOperation operation)
	{
		// get the bucket index based on the first n bits of the key, according
		// to the number of defined buckets
		int bucketId = DrumUtil.getBucketForKey(key, this.numBuckets);
				
		// add a new InMemoryData object to the broker
		this.inMemoryBuffer.get(bucketId).put(
				new InMemoryData<V,A>(key,value,aux,operation));
	}
	
	/**
	 * <p>Returns the name of the DRUM instance.</p>
	 * 
	 * @return The name of the DRUM instance
	 */
	public String getName()
	{
		return this.drumName;
	}
	
	/**
	 * <p>Returns the number of buckets used by this DRUM instance.</p>
	 * 
	 * @return The number of buckets used
	 */
	public int getNumberOfBuckets()
	{
		return this.numBuckets;
	}
}
