package at.rovo.drum;

import at.rovo.drum.datastore.DataStoreMerger;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

/**
 * Implementation of the build pattern presented by Joshua Bloch in his book 'Effective Java - Second Edition' in 'Item
 * 2: Consider a builder when faced with many constructor parameters'.
 * <p>
 * On invoking {@link #build()} the builder will create a new instance of DRUM with the provided parameters.
 * <p>
 * By default, the builder will create a DRUM instance for 512 buckets with 64k buffer size and a {@link
 * at.rovo.drum.NullDispatcher}.
 *
 * @param <V>
 *         The type of the value DRUM will manage
 * @param <A>
 *         The type of the auxiliary data attached to a key
 *
 * @author Roman Vottner
 */
public class DrumBuilder<V extends Serializable, A extends Serializable> implements Builder<Drum<V, A>>
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
    private int bufferSize = 64 * 1024;
    /** The class responsible for dispatching the results **/
    private Dispatcher<V, A> dispatcher = new NullDispatcher<>();
    /** A listener class which needs to be informed of state changes **/
    private DrumListener listener = null;
    /** The object that takes care of dispatching internal state changes to registered listeners **/
    private DrumEventDispatcher eventDispatcher = new DrumEventDispatcherImpl();
    /** The DRUM store factory class to initialize **/
    private Class<? extends DataStoreMerger> storeMergerClass = null;

    /**
     * Creates a new builder object with the minimum number of required data to instantiate a new {@link
     * DrumImpl DrumImpl} instance on invoking {@link #build()}.
     *
     * @param drumName
     *         The name of the DRUM instance
     * @param valueClass
     *         The type of the value this instance will manage
     * @param auxClass
     *         The type of the auxiliary data this instance will manage
     */
    public DrumBuilder(String drumName, Class<V> valueClass, Class<A> auxClass)
    {
        this.drumName = drumName;
        this.valueClass = valueClass;
        this.auxClass = auxClass;
    }

    /**
     * Assigns the builder to create a Drum instance which uses the provided dispatcher instead of the default {@link
     * NullDispatcher} to dispatch results.
     *
     * @param dispatcher
     *         The dispatcher to use for instantiating DRUM
     *
     * @return The builder responsible for creating a new instance of DRUM
     */
    public DrumBuilder<V, A> dispatcher(Dispatcher<V, A> dispatcher)
    {
        if (dispatcher == null)
        {
            throw new IllegalArgumentException("Invalid dispatcher received");
        }

        this.dispatcher = dispatcher;
        return this;
    }

    /**
     * Assigns the builder to create a Drum instance which uses the provided number of buckets instead of the default
     * 512 buckets.
     *
     * @param numBuckets
     *         The number of buckets DRUM should manage
     *
     * @return The builder responsible for creating a new instance of DRUM
     */
    public DrumBuilder<V, A> numBucket(int numBuckets)
    {
        if (numBuckets <= 0 || ((numBuckets & -numBuckets) != numBuckets))
        {
            throw new IllegalArgumentException(
                    "The number of buckets must be greater than 0 and must be a superset of 2");
        }

        this.numBuckets = numBuckets;
        return this;
    }

    /**
     * Assigns the builder to create a Drum instance which uses the provided buffer size instead of 64kb.
     *
     * @param bufferSize
     *         The buffer size DRUM should use before flushing the content
     *
     * @return The builder responsible for creating a new instance of DRUM
     */
    public DrumBuilder<V, A> bufferSize(int bufferSize)
    {
        if (bufferSize <= 0 || ((bufferSize & -bufferSize) != bufferSize))
        {
            throw new IllegalArgumentException(
                    "BufferSize must be greater than 0 and have a base of 2 (ex: 2^1, 2^2, 2^3, ...)");
        }

        this.bufferSize = bufferSize;
        return this;
    }

    /**
     * Assigns the builder to create a Drum instance with the defined listener in place.
     *
     * @param listener
     *         The listener to notify on state changes
     *
     * @return The builder responsible for creating a new instance of DRUM
     */
    public DrumBuilder<V, A> listener(DrumListener listener)
    {
        this.listener = listener;
        return this;
    }

    /**
     * Assigns the builder to create a Drum instance with a custom data store managed by the provided class.
     *
     * @param mergerClass
     *         A child class of {@link DataStoreMerger} which performs the uniqueness check against the backing data
     *         store
     *
     * @return The builder responsible for creating a new instance of DRUM
     */
    public DrumBuilder<V,A> datastore(Class<? extends DataStoreMerger> mergerClass)
    {
        this.storeMergerClass = mergerClass;
        return this;
    }

    /**
     * Assigns the builder to create a Drum instance with a custom event dispatcher object.
     *
     * @param eventDispatcher
     *         A reference to the object which should take care of shipping internal state events to registered listeners
     *
     * @return The builder responsible for creating a new instance of DRUM
     */
    public DrumBuilder<V, A> eventDispatcher(DrumEventDispatcher eventDispatcher)
    {
        this.eventDispatcher = eventDispatcher;
        return this;
    }

    /**
     * Assigns the builder to create and initialize a new instance of a DRUM object.
     *
     * @return A new initialized instance of DRUM
     *
     * @throws DrumException
     *         If during the initialization of DRUM an error occurred
     */
    public Drum<V, A> build() throws Exception
    {
        DataStoreMerger<V, A> merger;
        if (this.storeMergerClass != null)
        {
            try
            {
                Constructor constructor =
                        this.storeMergerClass.getConstructor(String.class, Class.class);
                //noinspection unchecked
                merger = (DataStoreMerger<V, A>)constructor.newInstance(this.drumName, this.valueClass);
            }
            catch (NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException ex)
            {
                ex.printStackTrace();
                throw new DrumException("Could not initialize data store merger based on provided class");
            }
        }
        else
        {
            throw new DrumException("No data store merger class found");
        }


        // in order to keep the interface for the builder clean, the builder is creating a settings object which is
        // only visible within the same package and initializes a new DRUM instance with the settings parameter object
        DrumSettings<V,A> settings = new DrumSettings<>(this.drumName, this.numBuckets, this.bufferSize,
                                                        this.valueClass, this.auxClass, this.dispatcher, this.listener,
                                                        this.eventDispatcher, merger);
        return new DrumImpl<>(settings);
    }
}
