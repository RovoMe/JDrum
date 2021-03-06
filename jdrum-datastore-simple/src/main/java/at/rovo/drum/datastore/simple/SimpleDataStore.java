package at.rovo.drum.datastore.simple;

import at.rovo.drum.DrumException;
import at.rovo.drum.DrumStoreEntry;
import at.rovo.drum.NotAppendableException;

import java.io.IOException;
import java.io.Serializable;

/**
 * A data store implementation will write {@link DrumStoreEntry} entries to a file on invoking either {@link
 * #writeEntry(DrumStoreEntry)} or {@link #writeEntry(DrumStoreEntry, boolean)} and retrieve a previously stored entry
 * using {@link #getEntry(Long)} based on a {@link DrumStoreEntry#getKey() key} contained within the data object.
 * Through {@link #getNextEntry()} the data entry at the current position of the cursor is returned. This method can be
 * used to iterate through the content of the data store sequentially.
 * <p>
 * {@link #reset()} will set the cursor of the data file back to the start while {@link #close()} will shut down the
 * data store and free any held resources.
 *
 * @param <V> The type of the value entry
 * @author Roman Vottner
 */
public interface SimpleDataStore<V extends Serializable> extends AutoCloseable {

    /**
     * Writes a new pair of key and value data into the cache file in a sorted order depending on the value of the key.
     * <p>
     * If a key with the same value exists, it will be overwritten to update the new value for this key. Existing
     * entries located after the data to write will be moved further backwards.
     *
     * @param data The data to write into the cache file
     * @return The updated entry
     * @throws IOException            If the backing cache file could not get accessed or throw an exception while
     *                                writing the entry to its storage
     * @throws ClassNotFoundException If the bytes of one of the entries in the cache file, the <em>data</em> argument
     *                                is compared with, belong to a different type than the value type of this data store
     * @throws NotAppendableException If the entry to write does not implement {@link at.rovo.drum.data.AppendableData}
     */
    DrumStoreEntry<V, ?> writeEntry(DrumStoreEntry<V, ?> data)
            throws IOException, ClassNotFoundException, NotAppendableException;

    /**
     * Writes a new pair of key and value data into the cache file in a sorted order depending on the value of the key.
     * <p>
     * If a key with the same value exists, it will be overwritten to update the new value for this key. Existing
     * entries located after the data to write will be moved further backwards.
     *
     * @param data   The data to write into the cache file
     * @param append Specifies if the data to write should be appended to an already existing entry with the same key.
     * @return The updated entry
     * @throws IOException            If the backing cache file could not get accessed or throw an exception while
     *                                writing the entry to its storage
     * @throws ClassNotFoundException If the bytes of one of the entries in the cache file, the <em>data</em> argument
     *                                is compared with, belong to a different type than the value type of this data store
     * @throws NotAppendableException If the entry to write does not implement {@link at.rovo.drum.data.AppendableData}
     */
    DrumStoreEntry<V, ?> writeEntry(DrumStoreEntry<V, ?> data, boolean append)
            throws IOException, ClassNotFoundException, NotAppendableException;

    /**
     * Returns the entry which matches the given <em>key</em> value.
     *
     * @param key The key value the entry should be retrieved for
     * @return The data value which is stored under the given key or null if no data with the given key could be found
     * @throws IOException            If the backing cache file could not get accessed or throw an exception while
     *                                reading the entry from its storage
     * @throws ClassNotFoundException If the bytes of the entry in the cache file, the <em>key</em> argument points to,
     *                                belong to a different type than the value type of this data store
     */
    DrumStoreEntry<V, ?> getEntry(Long key)
            throws IOException, ClassNotFoundException;

    /**
     * Returns the entry starting at the current cursor position. If the cursor is in the middle of an entry it will not
     * find any useful results! So make sure to set the cursor to the beginning of an entry first before invoking this
     * method!
     *
     * @return The entry at the current cursor position or null if either the end of the file was reached without
     * finding an entry or if the cursor was placed in the middle of an entry.
     * @throws IOException            If the backing cache file could not get accessed or throw an exception while
     *                                reading the entry from its storage
     * @throws ClassNotFoundException If the bytes of the entry in the cache file belong to a different type than the
     *                                value type of this data store
     */
    DrumStoreEntry<V, ?> getNextEntry()
            throws IOException, ClassNotFoundException;

    /**
     * Returns the length of the data store in bytes.
     *
     * @return The length of the data store in bytes
     * @throws DrumException If the length of the backing cache file could not get accessed due to various reasons
     */
    long length() throws DrumException;

    /**
     * Returns the number of entries in the cache file.
     *
     * @return The number of entries in the cache file
     */
    long getNumberOfEntries();

    /**
     * Resets the internal file pointer to the start of the file and clears cached data to simulate a new run-through.
     */
    void reset();
}
