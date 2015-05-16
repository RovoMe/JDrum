package at.rovo.caching.drum;

import at.rovo.caching.drum.data.ByteSerializer;
import at.rovo.caching.drum.internal.backend.DrumStorageFactory;

/**
 * Implementation of the build pattern presented by Joshua Bloch in his book 'Effective Java - Second Edition' in 'Item
 * 2: Consider a builder when faced with many constructor parameters'.
 * <p>
 * On invoking {@link #build()} the builder will create a new instance of DRUM with the provided parameters.
 * <p>
 * By default, the builder will create a DRUM instance for 512 buckets with 64k buffer size and a {@link
 * at.rovo.caching.drum.NullDispatcher}.
 *
 * @param <V>
 *         The type of the value DRUM will manage
 * @param <A>
 *         The type of the auxiliary data attached to a key
 *
 * @author Roman Vottner
 */
@SuppressWarnings("unused")
public class DrumBuilder<V extends ByteSerializer<V>, A extends ByteSerializer<A>> implements Builder<Drum<V, A>>
{
    // required parameters
    /** The name of the drum instance **/
    private final String drumName;
    /** The type of the value managed by DRUM **/
    private final Class<? super V> valueClass;
    /** The type of the auxiliary data managed by DRUM **/
    private final Class<? super A> auxClass;

    /** The number of buckets managed by this DRUM instance **/
    private int numBuckets = 512;
    /** The size of the buffer before a flush is forced **/
    private int bufferSize = 64 * 1024;
    /** The class responsible for dispatching the results **/
    private Dispatcher<V, A> dispatcher = new NullDispatcher<>();
    /** A listener class which needs to be informed of state changes **/
    private DrumListener listener = null;
    /** The factory which creates the backing storage service **/
    private DrumStorageFactory<V, A> factory = null;

    /**
     * Creates a new builder object with the minimum number of required data to instantiate a new {@link
     * at.rovo.caching.drum.DrumImpl DrumImpl} instance on invoking {@link #build()}.
     *
     * @param drumName
     *         The name of the DRUM instance
     * @param valueClass
     *         The type of the value this instance will manage
     * @param auxClass
     *         The type of the auxiliary data this instance will manage
     */
    public DrumBuilder(String drumName, Class<? super V> valueClass, Class<? super A> auxClass)
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
     * Assigns the builder to create a Drum instance with the given factory to create a backend storage instead of the
     * backend storage created by the default factory.
     *
     * @param factory
     *         The factory responsible for creating the backend storage
     *
     * @return The builder responsible for creating a new instance of DRUM
     */
    public DrumBuilder<V, A> factory(DrumStorageFactory<V, A> factory)
    {
        this.factory = factory;
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
        return new DrumImpl<>(this);
    }

    /**
     * Returns the name of the DRUM instance to create.
     *
     * @return The name of the DRUM instance
     */
    String getDrumName()
    {
        return this.drumName;
    }

    /**
     * Returns the class type of the value assigned to the DRUM instance to build.
     *
     * @return The class type of the value elements
     */
    Class<? super V> getValueClass()
    {
        return this.valueClass;
    }

    /**
     * Returns the class type of the auxiliary data assigned to the DRUM instance to build.
     *
     * @return The class type of the auxiliary data elements
     */
    Class<? super A> getAuxClass()
    {
        return this.auxClass;
    }

    /**
     * Returns the number of buckets to use for the DRUM instance to build.
     *
     * @return The number of buckets to use for DRUM
     */
    int getNumBuckets()
    {
        return this.numBuckets;
    }

    /**
     * Returns the byte size of the buffer at which a merge between the bucket files an the backing data store occurs
     * for the DRUM instance to create.
     *
     * @return The size of the buffer at which a merge will occur.
     */
    int getBufferSize()
    {
        return this.bufferSize;
    }

    /**
     * Returns the assigned dispatcher for the DRUM instance to create.
     *
     * @return The assigned dispatcher for the DRUM instance
     */
    Dispatcher<V, A> getDispatcher()
    {
        return this.dispatcher;
    }

    /**
     * Returns the specified listener for the DRUM instance.
     *
     * @return The specified listener to use with the DRUM instance to create
     */
    DrumListener getListener()
    {
        return this.listener;
    }

    /**
     * Returns the factory which creates the backing storage service for the DRUM instance to create.
     *
     * @return The factory for the backing storage service assigned to DRUM
     */
    DrumStorageFactory<V, A> getFactory()
    {
        return this.factory;
    }
}
