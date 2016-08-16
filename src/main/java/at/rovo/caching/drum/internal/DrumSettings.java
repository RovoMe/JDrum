package at.rovo.caching.drum.internal;

import at.rovo.caching.drum.Dispatcher;
import at.rovo.caching.drum.Drum;
import at.rovo.caching.drum.DrumListener;
import at.rovo.caching.drum.DrumEventDispatcher;
import at.rovo.caching.drum.DrumStoreFactory;
import java.io.Serializable;

/**
 * This immutable class encapsulates the configuration settings which should be used to initialize a new {@link Drum}
 * instance with.
 *
 * @param <V>
 *         The type of the value DRUM will manage
 * @param <A>
 *         The type of the auxiliary data attached to a key
 *
 * @author Roman Vottner
 */
public class DrumSettings<V extends Serializable, A extends Serializable>
{
    /** The name of the DRUM instance **/
    private final String drumName;
    /** The number of buckets to initialize **/
    private final int numBuckets;
    /** The byte size before an exceeded event is triggered **/
    private final int bufferSize;
    /** The class of the value object **/
    private final Class<V> valueClass;
    /** The class of the auxiliary data object **/
    private final Class<A> auxClass;
    /** The dispatcher to send responses with **/
    private final Dispatcher<V,A> dispatcher;
    /** The listener to inform on internal state changes **/
    private final DrumListener listener;
    /** The factory used to create the backing data store with **/
    private final DrumStoreFactory<V,A> factory;
    /** The object which is dispatching the internal state events **/
    private final DrumEventDispatcher eventDispatcher;

    /**
     * Creates a new immutable instance containing configuration settings for the {@link Drum} instance to initialize.
     *
     * @param drumName
     *         The name of the DRUM instance
     * @param numBuckets
     *         The number of buckets to initialize
     * @param bufferSize
     *         The size of the buckets before an exceeded event is triggered
     * @param valueClass
     *         The class object of the value object
     * @param auxClass
     *         The class object of the auxiliary data object
     * @param dispatcher
     *         The dispatcher to send results with
     * @param listener
     *         The listener to inform on internal state changes
     * @param factory
     *         The factory to initialize the backing data store with
     */
    public DrumSettings(String drumName, int numBuckets, int bufferSize, Class<V> valueClass, Class<A> auxClass,
                        Dispatcher<V, A> dispatcher, DrumListener listener, DrumEventDispatcher eventDispatcher,
                        DrumStoreFactory<V, A> factory) {
        this.drumName = drumName;
        this.numBuckets = numBuckets;
        this.bufferSize = bufferSize;
        this.valueClass = valueClass;
        this.auxClass = auxClass;
        this.dispatcher = dispatcher;
        this.listener = listener;
        this.eventDispatcher = eventDispatcher;
        this.factory = factory;
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
    Class<V> getValueClass()
    {
        return this.valueClass;
    }

    /**
     * Returns the class type of the auxiliary data assigned to the DRUM instance to build.
     *
     * @return The class type of the auxiliary data elements
     */
    Class<A> getAuxClass()
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
    DrumStoreFactory<V, A> getFactory()
    {
        return this.factory;
    }

    /**
     * Returns a reference to the object which is taking care of dispatching internal state events to registered
     * listeners.
     *
     * @return A reference to the event dispatcher object
     */
    DrumEventDispatcher getEventDispatcher() {
        return this.eventDispatcher;
    }
}
