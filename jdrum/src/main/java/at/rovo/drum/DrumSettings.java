package at.rovo.drum;

import at.rovo.drum.datastore.DataStoreMerger;
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
    /** The data store merger to use to check entries for their uniqueness **/
    private final DataStoreMerger<V,A> dataStoreMerger;
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
     * @param dataStoreMerger
     *         The factory to initialize the backing data store with
     */
    public DrumSettings(String drumName, int numBuckets, int bufferSize, Class<V> valueClass, Class<A> auxClass,
                        Dispatcher<V, A> dispatcher, DrumListener listener, DrumEventDispatcher eventDispatcher,
                        DataStoreMerger<V, A> dataStoreMerger) {
        this.drumName = drumName;
        this.numBuckets = numBuckets;
        this.bufferSize = bufferSize;
        this.valueClass = valueClass;
        this.auxClass = auxClass;
        this.dispatcher = dispatcher;
        this.listener = listener;
        this.eventDispatcher = eventDispatcher;
        this.dataStoreMerger = dataStoreMerger;
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
     * Returns the data store merger instance to use for determining the uniqueness of entries against the backing data
     * store.
     *
     * @return The factory for the backing storage service assigned to DRUM
     */
    DataStoreMerger<V, A> getDataStoreMerger()
    {
        return this.dataStoreMerger;
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
