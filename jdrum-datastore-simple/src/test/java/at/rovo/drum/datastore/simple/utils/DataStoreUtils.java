package at.rovo.drum.datastore.simple.utils;

import at.rovo.common.Pair;
import at.rovo.drum.DrumException;
import at.rovo.drum.datastore.simple.SimpleDataStore;
import at.rovo.drum.util.DrumUtils;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * This utility class provides convenient methods to interact with the {@link SimpleDataStore}.
 *
 * @author Roman Vottner
 */
public final class DataStoreUtils
{
    private final static Logger LOG = LogManager.getLogger(MethodHandles.lookup().lookupClass());

    /**
     * Iterates through the content of the data store and returns a list of contained hash keys for the respective
     * entries.
     *
     * @param name
     *         The name of the backing data store
     * @param valueClass
     *         The data type of the value object associated to the key
     * @param <V>
     *         The type of the contained value object
     *
     * @return Returns the keys of the contained objects in the data store
     *
     * @throws IOException
     *         If any error during reading the data store occurs
     * @throws DrumException
     *         Thrown if the next entry from the data store could not be extracted
     */
    public static <V> List<Long> getStoredKeys(String name, Class<? extends V> valueClass)
            throws IOException, DrumException
    {
        Map<Long, V> content = getContentAsMap(name, valueClass);
        List<Long> keys = new ArrayList<>(content.size());
        keys.addAll(content.keySet());
        return keys;
    }

    public static <V> void printCacheContent(Map<Long, V> content)
    {
        LOG.debug("Data contained in cache file: ");
        for (Long key : content.keySet()) {
            LOG.debug("Key: {}, Value: {}", key, content.get(key));
        }
    }

    public static <V> void printCacheContent(String name, Class<? extends V> valueClass)
            throws IOException, DrumException
    {
        Map<Long, V> content = getContentAsMap(name, valueClass);
        printCacheContent(content);
    }

    /**
     * Iterates through the content of the data store and adds each entry as new entry to a {@link Map} with the entries
     * hash key as key and the corresponding data entry as value.
     *
     * @param name
     *         The name of the backing data store
     * @param valueClass
     *         The data type of the value object associated to the key
     * @param <V>
     *         The type of the contained value object
     *
     * @return Returns the content of the data store as a map object
     *
     * @throws IOException
     *         If any error during reading the data store occurs
     * @throws DrumException
     *         Thrown if the next entry from the data store could not be extracted
     */
    public static <V> Map<Long, V> getContentAsMap(String name, Class<? extends V> valueClass)
            throws IOException, DrumException
    {
        LOG.info("Data contained in {}/cache.db:", name);

        Map<Long, V> content = new LinkedHashMap<>();
        try (RandomAccessFile dataStore = DataStoreUtils.openDataStore(name))
        {
            dataStore.seek(0);

            Pair<Long, V> data = DataStoreUtils.getNextEntry(dataStore, valueClass);
            for (; data != null; data = DataStoreUtils.getNextEntry(dataStore, valueClass))
            {
                V value = data.getLast();
                content.put(data.getFirst(), value);
                if (value != null)
                {
                    LOG.info("Key: {}, Value: {}", data.getFirst(), value);
                }
                else
                {
                    LOG.info("Key: {}, Value: {}", data.getFirst(), null);
                }
            }
        }
        return content;
    }

    /**
     * Opens the backing data store and returns a {@link RandomAccessFile} reference to the opened file.
     *
     * @param name The name of the DRUM instance the data store should be opened for
     * @return A reference to the random access file
     * @throws IOException If the file could not be accessed
     */
    public static RandomAccessFile openDataStore(String name) throws IOException
    {
        String userDir = System.getProperty("user.dir");
        String cacheName = userDir + "/cache/" + name + "/cache.db";
        return new RandomAccessFile(cacheName, "r");
    }

    /**
     * Returns the next key/value pair from the backing data store.
     *
     * @param cacheFile
     *         The name of the backing data store
     * @param valueClass
     *         The data type of the value object associated to the key
     * @param <V>
     *         The type of the contained value object
     *
     * @return A key/value tuple
     *
     * @throws DrumException
     *         Thrown if either an IOException occurs during fetching the next Entry or the data can't be deserialized
     *         from bytes to an actual object
     */
    public static <V> Pair<Long, V> getNextEntry(RandomAccessFile cacheFile, Class<? extends V> valueClass)
            throws DrumException
    {
        // Retrieve the key from the file
        try
        {
            if (cacheFile.getFilePointer() >= cacheFile.length())
            {
                return null;
            }

            Long key = cacheFile.readLong();

            // Retrieve the value from the file
            int valueSize = cacheFile.readInt();
            if (valueSize > 0)
            {
                byte[] byteValue = new byte[valueSize];
                cacheFile.read(byteValue);
                V value;
                // as we have our own serialization mechanism, we have to ensure that these objects are serialized
                // appropriately
                if (Serializable.class.isAssignableFrom(valueClass))
                {
                    value = DrumUtils.deserialize(byteValue, valueClass);
                }
                // should not happen - but in case we refactor again leave it in
                else
                {
                    throw new DrumException("Could not read next entry as value was null before reading its bytes");
                }
                return new Pair<>(key, value);
            }
            return new Pair<>(key, null);
        }
        catch (IOException | ClassNotFoundException e)
        {
            throw new DrumException("Error fetching next entry from cache", e);
        }
    }
}
