package at.rovo.drum.impl;

import at.rovo.drum.Dispatcher;
import at.rovo.drum.DrumEventDispatcher;
import at.rovo.drum.DrumException;
import at.rovo.drum.Merger;

import java.io.Serializable;

/**
 * A basic Factory class for creating a DRUM backing data storage.
 *
 * @param <V> The type of the value
 * @param <A> The type of the auxiliary data attached to a key
 * @author Roman Vottner
 */
public abstract class DrumStoreFactory<V extends Serializable, A extends Serializable> {

    /**
     * The created merger implementation
     */
    protected Merger merger = null;

    /**
     * On creating a new instance of this class, it will trigger {@link #create(String, int, Dispatcher, Class, Class,
     * DrumEventDispatcher)} and therefore delegate the creation of the concrete implementation to its child class.
     *
     * @param drumName        The name of the DRUM instance
     * @param numBuckets      The number of bucket files used to store the data
     * @param dispatcher      A reference to the {@link Dispatcher} instance that will dispatch the results
     * @param valueClass      The class of the value type
     * @param auxClass        The class of the auxiliary data type
     * @param eventDispatcher A reference to the {@link DrumEventDispatcher} that will forward certain DRUM events like merge status
     *                        changed or disk writer events
     * @throws DrumException If the backing data store could not be created
     */
    public DrumStoreFactory(String drumName, int numBuckets, Dispatcher<V, A> dispatcher, Class<V> valueClass,
                            Class<A> auxClass, DrumEventDispatcher eventDispatcher) throws DrumException {
        this.create(drumName, numBuckets, dispatcher, valueClass, auxClass, eventDispatcher);
    }

    /**
     * Forces a subclass to implement the creation of a new instance of a backing data storage.
     *
     * @param drumName        The name of the DRUM instance
     * @param numBuckets      The number of bucket files used to store the data
     * @param dispatcher      A reference to the {@link Dispatcher} instance that will dispatch the results
     * @param valueClass      The class of the value type
     * @param auxClass        The class of the auxiliary data type
     * @param eventDispatcher A reference to the {@link DrumEventDispatcher} implementation that will forward certain DRUM events like
     *                        merge status changed or disk writer events
     * @throws DrumException If the backing data store could not be created
     */
    protected abstract void create(String drumName, int numBuckets, Dispatcher<V, A> dispatcher, Class<V> valueClass,
                                   Class<A> auxClass, DrumEventDispatcher eventDispatcher) throws DrumException;

    /**
     * Returns the generated data storage.
     *
     * @return The generated data storage
     */
    public Merger getStorage() {
        return this.merger;
    }

//    /**
//     * Create the default backing data store.
//     *
//     * @param drumName
//     *         The name of the DRUM instance
//     * @param numBuckets
//     *         The number of bucket files used to store the data
//     * @param dispatcher
//     *         A reference to the {@link at.rovo.drum.Dispatcher} instance that will dispatch the results
//     * @param valueClass
//     *         The class of the value type
//     * @param auxClass
//     *         The class of the auxiliary data type
//     * @param eventDispatcher
//     *         A reference to the {@link DrumEventDispatcher} implementation that will forward certain DRUM events like
//     *         merge status changed or disk writer events
//     *
//     * @return A reference to the default backing data storage, which is the CacheFile data store
//     *
//     * @throws DrumException
//     *         If the backing data store could not be created
//     */
//    static <V extends Serializable, A extends Serializable> DrumStoreFactory<V, A> getDefaultStorageFactory(
//            String drumName, int numBuckets, Dispatcher<V, A> dispatcher, Class<V> valueClass, Class<A> auxClass,
//            DrumEventDispatcher eventDispatcher) throws DrumException
//    {
//        return new DataStoreMergerFactory<>(drumName, numBuckets, dispatcher, valueClass, auxClass, eventDispatcher);
//    }
}
