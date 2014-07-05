package at.rovo.caching.drum;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import at.rovo.caching.drum.data.ByteSerializer;
import at.rovo.caching.drum.event.DrumEventDispatcher;
import at.rovo.caching.drum.internal.DiskBucketWriter;
import at.rovo.caching.drum.internal.InMemoryData;
import at.rovo.caching.drum.internal.InMemoryMessageBroker;
import at.rovo.caching.drum.internal.backend.DrumStorageFactory;
import at.rovo.caching.drum.util.DrumExceptionHandler;
import at.rovo.caching.drum.util.DrumUtil;
import at.rovo.caching.drum.util.NamedThreadFactory;

/**
 * <p>
 * This implementation of the 'Disk Repository with Update Management' structure
 * utilizes a consumer/producer pattern to store and process input received by
 * its
 * </p>
 * <ul>
 * <li>{@link #check(Long)} or {@link #check(Long, A)}</li>
 * <li>{@link #update(Long, V)} or {@link #update(Long, V, A)}</li>
 * <li>{@link #checkUpdate(Long, V)} or {@link #checkUpdate(Long, V, A)}</li>
 * </ul>
 * <p>
 * methods.
 * </p>
 * <p>
 * Internally <code>numBuckets</code> buffers and buckets will be created which
 * will hold the data sent to DRUM. Buffers are the in-memory storage while
 * buckets are the intermediary disk files. Buffers fill up to <code>
 * bufferSize</code> bytes before they get sent to a disk file.
 * </p>
 * 
 * 
 * @param <V>
 *            The type of the value
 * @param <A>
 *            The type of the auxiliary data attached to a key
 * 
 * @author Roman Vottner
 */
@SuppressWarnings("unused")
public class Drum<V extends ByteSerializer<V>, A extends ByteSerializer<A>>
		implements IDrum<V, A>
{
	/** The logger of this class **/
	private final static Logger LOG = LogManager.getLogger(Drum.class);

	/** The name of the DRUM instance **/
	protected String drumName = null;
	/** The number of buffers and buckets used **/
	protected int numBuckets = 0;
	/** The size of an in-memory buffer **/
	protected int bufferSize = 0;
	/**
	 * The broker list which holds elements in memory until they get written to
	 * the disk file
	 **/
	protected List<IBroker<InMemoryData<V, A>, V, A>> inMemoryBuffer = null;
	/**
	 * The set of writer objects that listens to notifications of a broker and
	 * write content from the broker to a disk file
	 **/
	protected List<IDiskWriter<V, A>> diskWriters = null;
	/**
	 * The object that compares keys of data-objects for their uniqueness and
	 * merges them into the data store in case the need to be updated
	 **/
	protected IMerger<V, A> merger = null;
	/** The execution service which hosts our threads **/
	protected ExecutorService executor = null;
	/** The merger thread **/
	protected Thread mergerThread = null;
	/**
	 * The event dispatcher used to inform listeners of internal state changes
	 * and certain statistics
	 **/
	protected DrumEventDispatcher eventDispatcher = new DrumEventDispatcher();
	/** The event dispatcher thread **/
	protected Thread eventDispatcherThread = null;
	
	/**
	 * <p>
	 * Implementation of the build pattern presented by Joshua Bloch in his book
	 * 'Effective Java - Second Edition' in 'Item 2: Consider a builder when
	 * faced with many constructor parameters'. 
	 * </p>
	 * <p>
	 * On invoking {@link #build()} the builder will create a new instance of 
	 * DRUM with the provided parameters.
	 * </p>
	 * <p>
	 * By default, the builder will create a DRUM instance for 512 buckets with
	 * 64k buffer size and a {@link NullDispatcher}.
	 * </p>
	 * 
	 * @param <V>
	 *            The type of the value DRUM will manage
	 * @param <A>
	 *            The type of the auxiliary data attached to a key
	 * 
	 * @author Roman Vottner
	 */
	public static class Builder<V extends ByteSerializer<V>, A extends ByteSerializer<A>>
		implements IBuilder<Drum<V, A>>
	{
		// required parameters
		/** The name of the drum instance **/
		private final String drumName;
		/** The type of the value managed by DRUM **/
		private final Class<V> valueClass;
		/** The type of the auxiliary data managed by DRUM **/
		private final Class<A> auxClass;
		
		/** The number of buckets managed by this DRUM instance **/
		private int numBuckets = 512;
		/** The size of the buffer before a flush is forced **/
		private int bufferSize = 64*1024;
		/** The class responsible for dispatching the results **/
		private IDispatcher<V,A> dispatcher = new NullDispatcher<>();
		/** A listener class which needs to be informed of state changes **/
		private IDrumListener listener = null;
		/** The factory which creates the backing storage service **/
		private DrumStorageFactory<V,A> factory = null;
		
		/**
		 * <p>
		 * Creates a new builder object with the minimum number of required data
		 * to instantiate a new {@link Drum} instance on invoking 
		 * {@link #build()}.
		 * </p>
		 * 
		 * @param drumName The name of the DRUM instance
		 * @param valueClass The type of the value this instance will manage
		 * @param auxClass The type of the auxiliary data this instance will 
		 *                 manage
		 */
		public Builder(String drumName, Class<V> valueClass, Class<A> auxClass)
		{
			this.drumName = drumName;
			this.valueClass = valueClass;
			this.auxClass = auxClass;
		}
		
		/**
		 * <p>
		 * Assigns the builder to create a Drum instance which uses the provided 
		 * dispatcher instead of the default {@link NullDispatcher} to dispatch 
		 * results.
		 * </p>
		 * 
		 * @param dispatcher The dispatcher to use for instantiating DRUM
		 * @return The builder responsible for creating a new instance of DRUM
		 */
		public Builder<V,A> dispatcher(IDispatcher<V,A> dispatcher)
		{
			if (dispatcher == null)
				throw new IllegalArgumentException("Invalid dispatcher received");
			
			this.dispatcher = dispatcher;
			return this;
		}
		
		/**
		 * <p>
		 * Assigns the builder to create a Drum instance which uses the provided
		 * number of buckets instead of the default 512 buckets.
		 * </p>
		 * 
		 * @param numBuckets The number of buckets DRUM should manage
		 * @return The builder responsible for creating a new instance of DRUM
		 */
		public Builder<V,A> numBucket(int numBuckets)
		{
			if (numBuckets <= 0 || ((numBuckets & -numBuckets) != numBuckets))
				throw new IllegalArgumentException(
						"The number of buckets must be greater than 0 and must "
						+ "be a superset of 2");
			
			this.numBuckets = numBuckets;
			return this;
		}
		
		/**
		 * <p>
		 * Assigns the builder to create a Drum instance which uses the provided
		 * buffer size instead of 64kb.
		 * </p>
		 * 
		 * @param bufferSize The buffer size DRUM should use before flushing the 
		 *                   content
		 * @return The builder responsible for creating a new instance of DRUM
		 */
		public Builder<V,A> bufferSize(int bufferSize)
		{
			if (bufferSize <= 0 || ((bufferSize & -bufferSize) != bufferSize))
				throw new IllegalArgumentException(
						"BufferSize must be greater than 0 and have a base of 2 "
						+ "(ex: 2^1, 2^2, 2^3, ...)");
			
			this.bufferSize = bufferSize;
			return this;
		}
		
		/**
		 * <p>
		 * Assigns the builder to create a Drum instance with the defined 
		 * listener in place.
		 * </p>
		 * 
		 * @param listener The listener to notify on state changes
		 * @return The builder responsible for creating a new instance of DRUM
		 */
		public Builder<V,A> listener(IDrumListener listener)
		{
			this.listener = listener;
			return this;
		}
		
		/**
		 * <p>
		 * Assigns the builder to create a Drum instance with the given factory 
		 * to create a backend storage instead of the backend storage created by 
		 * the default factory.
		 * </p>
		 * 
		 * @param factory The factory responsible for creating the backend 
		 *                storage
		 * @return The builder responsible for creating a new instance of DRUM
		 */
		public Builder<V,A> factory(DrumStorageFactory<V,A> factory)
		{
			this.factory = factory;
			return this;
		}
		
		/**
		 * <p>
		 * Assigns the builder to create and initialize a new instance of a DRUM
		 * object.
		 * </p>
		 * 
		 * @return A new initialized instance of DRUM
		 * @throws DrumException If during the initialization of DRUM an error 
		 *                       occurred
		 */
		public Drum<V,A> build() throws Exception
		{
			return new Drum<>(this);
		}
	}
	
	/**
	 * <p>
	 * Creates a new instance and assigns initial values contained within the 
	 * builder object to the corresponding attributes.
	 * </p>
	 *
	 * @param builder The builder object which contains the initialization
	 *                parameters specified by the invoker
	 * @throws DrumException If during the initialization of the backing data
	 *                       store an error occurred
	 */
	private Drum(Builder<V,A> builder) throws DrumException
	{
		if (builder.listener != null)
			this.addDrumListener(builder.listener);
		
		DrumStorageFactory<V,A> factory;
		if (builder.factory != null)
			factory = builder.factory;
		else
			factory = DrumStorageFactory.getDefaultStorageFactory(
					builder.drumName, builder.numBuckets, builder.dispatcher, 
					builder.valueClass, builder.auxClass, this.eventDispatcher);
		
		this.init(builder.drumName, builder.numBuckets, builder.bufferSize, 
				builder.dispatcher, builder.valueClass,	builder.auxClass, 
				factory);
	}


	/**
	 * <p>
	 * Initializes the DRUM instance with required data and starts the worker
	 * threads.
	 * </p>
	 * 
	 * @param drumName
	 *            The name of the DRUM instance
	 * @param numBuckets
	 *            The number of buckets to be used
	 * @param bufferSize
	 *            The size of a single buffer in bytes
	 * @param dispatcher
	 *            The {@link IDispatcher} implementation which will receive
	 *            information on items added via <code>check</code>,
	 *            <code>update</code> or <code>checkUpdate</code>.
	 * @param valueClass
	 *            The class-type of the value for a certain key
	 * @param auxClass
	 *            The auxiliary data-type attached to a certain key
	 * @param factory
	 *            The factory object which defines where data should be stored
	 *            in. Note that factory must return an implementation of IMerger
	 * @throws DrumException
	 */
	private void init(String drumName, int numBuckets, int bufferSize,
			IDispatcher<V, A> dispatcher, Class<V> valueClass,
			Class<A> auxClass, DrumStorageFactory<V, A> factory)
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
		this.executor = Executors.newFixedThreadPool(this.numBuckets,
				writerFactory);

		for (int i = 0; i < numBuckets; i++)
		{
			IBroker<InMemoryData<V, A>, V, A> broker = new InMemoryMessageBroker<>(
					drumName, i, bufferSize, this.eventDispatcher);
			IDiskWriter<V, A> consumer = new DiskBucketWriter<>(drumName,
					i, bufferSize, broker, this.merger, this.eventDispatcher);

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
		for (IBroker<?, ?, ?> broker : this.inMemoryBuffer)
			broker.stop();

		// give the threads a chance to finish their work without being
		// interrupted
		for (IDiskWriter<V, A> writer : this.diskWriters)
			writer.stop();

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
			e.printStackTrace();
		}

		// close the open resources held by the writers
		for (IDiskWriter<V, A> writer : this.diskWriters)
			writer.close();

		this.eventDispatcher.stop();
		this.eventDispatcherThread.interrupt();
		LOG.trace("[{}] - disposed", this.drumName);
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
	 * <p>
	 * Stores the key, the value and the auxiliary data as well as the operation
	 * to be executed on these data in the according in-memory buffer.
	 * </p>
	 * 
	 * @param key
	 *            The hash value of the data
	 * @param value
	 *            The value associated with the key
	 * @param aux
	 *            The auxiliary data of the key
	 * @param operation
	 *            The operation to be used on the data
	 */
	private void add(Long key, V value, A aux, DrumOperation operation)
	{
		// get the bucket index based on the first n bits of the key, according
		// to the number of defined buckets
		int bucketId = DrumUtil.getBucketForKey(key, this.numBuckets);

		// add a new InMemoryData object to the broker
		this.inMemoryBuffer.get(bucketId).put(
				new InMemoryData<>(key, value, aux, operation));
	}

	/**
	 * <p>
	 * Returns the name of the DRUM instance.
	 * </p>
	 * 
	 * @return The name of the DRUM instance
	 */
	public String getName()
	{
		return this.drumName;
	}

	/**
	 * <p>
	 * Returns the number of buckets used by this DRUM instance.
	 * </p>
	 * 
	 * @return The number of buckets used
	 */
	public int getNumberOfBuckets()
	{
		return this.numBuckets;
	}
}
