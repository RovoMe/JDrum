package at.rovo.caching.drum.util;

import at.rovo.caching.drum.DrumException;
import at.rovo.caching.drum.data.ByteSerializer;
import at.rovo.common.Pair;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.lang.invoke.MethodHandles;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * This utility class provides some basic methods to convert 32bit integer values to a byte-array and vice versa and to
 * extract 64bit hash codes from String objects and get the number of a bucket based on a key and the number of buckets
 * for 32- and 64bit calculations.
 * <p>
 * Note however, that Java only supports lists with 32bit integer length - therefore the 64bit versions here aren't used
 * in the application itself.
 *
 * @author Roman Vottner
 */
@SuppressWarnings("unused")
public class DrumUtils
{
    /** The logger of this class **/
    private final static Logger LOG = LogManager.getLogger(MethodHandles.lookup().lookupClass());

    /**
     * Calculates a 8-byte (64bit) hash from a {@link String}
     *
     * @param string
     *         The string object to generate the 64bit hash value for
     *
     * @return The 64bit long hash value for the provided string
     */
    public static long hash(final String string)
    {
        long h = 1125899906842597L; // prime
        int len = string.length();

        for (int i = 0; i < len; i++)
        {
            h = 31 * h + string.charAt(i);
        }
        return h;
    }

    /**
     * Calculates a 8-byte (64bit) hash from an object by invoking the objects {@link Object#toString()} first and then
     * generating the hash value for the generated string value.
     *
     * @param object
     *         The object whose key shall be calculated
     *
     * @return The 64bit long hash value for the provided object
     */
    public static long hash(final Object object)
    {
        return hash(object.toString());
    }

    /**
     * Calculates the bucket id the key should be placed in according to the first n bits of the key where n is the
     * power of 2 numBuckets is defined as. F.e if 1024 buckets are defined the first 10 bits of the key will be taken
     * as the index for the actual bucket (2^10 = 1024, 2^9 = 512, ...)
     *
     * @param key
     *         The 64bit key whose bucket index should be calculated
     * @param numBuckets
     *         The total number of available buckets. This should be a power of two (e.g. 2, 4, 8, 16, 32, ...)
     *
     * @return The bucket index the key should be in
     */
    public static int getBucketForKey(long key, final int numBuckets)
    {
        // test if numBuckets is a power of 2
        int exponent = Math.getExponent(numBuckets);
        if (numBuckets != Math.pow(2, exponent))
        {
            throw new IllegalArgumentException("Number of buckets does not correspond to a power of 2!");
        }
        return (int) (key >> (64 - exponent)) + numBuckets / 2;
    }

    /**
     * Calculates the bucket based on a 64bit key.
     * <p>
     * The calculation is based on an example presented by Leandro T C Melo
     *
     * @param key
     *         The key whose bucket should be calculated
     * @param numBuckets
     *         The total number of available buckets. This should be a power of two (e.g. 2, 4, 8, 16, 32, ...)
     *
     * @return The bucket the key should be in
     *
     * @link http://www.codeproject.com/Articles/36221/DRUM-A-C-Implementation-for-the-URL-seen-Test-of-a
     */
    public static long getBucketOfKey(final long key, final int numBuckets)
    {
        // test if numBuckets is a power of 2
        int exponent = Math.getExponent(numBuckets);
        if (numBuckets != Math.pow(2, exponent))
        {
            throw new IllegalArgumentException("Number of buckets does not correspond to a power of 2!");
        }
        // Build mask-string
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < 64; i++)
        {
            if (i < exponent)
            {
                builder.append("1");
            }
            else
            {
                builder.append("0");
            }
        }
        long mask = new BigInteger(builder.toString(), 2).longValue();
        long bucket = mask & key;
        return (bucket >>> (64 - exponent));
    }

    /**
     * Calculates the bucket based on a 32bit key.
     * <p>
     * The calculation is based on an example presented by Leandro T C Melo
     *
     * @param key
     *         The key whose bucket should be calculated
     * @param numBuckets
     *         The total number of available buckets. This should be a power of two (e.g. 2, 4, 8, 16, 32, ...)
     *
     * @return The bucket the key should be in
     *
     * @link http://www.codeproject.com/Articles/36221/DRUM-A-C-Implementation-for-the-URL-seen-Test-of-a
     */
    public static int getBucketOfKey(final int key, final int numBuckets)
    {
        // test if numBuckets is a power of 2
        int exponent = Math.getExponent(numBuckets);
        if (numBuckets != Math.pow(2, exponent))
        {
            throw new IllegalArgumentException("Number of buckets does not correspond to a power of 2!");
        }
        // Build mask-string
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < 32; i++)
        {
            if (i < exponent)
            {
                builder.append("1");
            }
            else
            {
                builder.append("0");
            }
        }
        int mask = new BigInteger(builder.toString(), 2).intValue();
        int bucket = mask & key;
        return (bucket >>> (32 - exponent));
    }

    /**
     * Serializes an object into a byte-array
     *
     * @param obj
     *         The object to convert into a binary array
     *
     * @return The bytes of the object or an empty byte array if the source object was null
     *
     * @throws IOException
     *         If any error during the serialization occurs
     */
    public synchronized static byte[] serialize(Object obj) throws IOException
    {
        if (obj instanceof String)
        {
            return obj.toString().getBytes();
        }

        // useful for Java objects - simple Integer objects create
        // up to 81 bytes instead of only 4!
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        if (obj == null)
        {
            byte[] ret = new byte[0];
            byte empty = 0;
            Arrays.fill(ret, 0, 0, empty);
            return ret;
        }
        oos.writeObject(obj);
        oos.flush();
        byte[] bytes = baos.toByteArray();

        oos.reset();

        oos.close();
        baos.close();

        return bytes;
    }

    /**
     * Turns a byte-array into an object
     *
     * @param bytes
     *         The bytes of the object
     *
     * @return The object deserialized from the array of bytes provided
     *
     * @throws IOException
     * @throws ClassNotFoundException
     */
    @SuppressWarnings("unchecked")
    public static <V extends ByteSerializer> V deserialize(byte[] bytes, Class<? super V> type)
            throws IOException, ClassNotFoundException
    {
        V ret;
        // check if the byte array is a String (character array)
        if (type.isAssignableFrom(String.class))
        {
            ret = ((V) type.cast(new String(bytes)));
        }
        else
        {
            ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
            ObjectInputStream ois = new ObjectInputStream(bais);
            // ois.mark(0);
            Object obj = ois.readObject();
            ret = ((V) type.cast(obj));

            if (ois.markSupported())
            {
                ois.reset();
            }
            ois.close();

            if (bais.markSupported())
            {
                bais.reset();
            }
            bais.close();
        }
        return ret;
    }

    /**
     * Converts a long value into a 8 byte (64bit) long byte array.
     *
     * @param l
     *         The long value to be converted into a byte array
     *
     * @return the 8-byte array representing the long value
     */
    public static byte[] long2bytes(long l)
    {
        byte[] result = new byte[8];

        for (int i = 0; i < result.length; i++)
        {
            result[i] = (byte) (l >> ((8 * result.length) - (8 * (i + 1))));
        }
        return result;
    }

    /**
     * Converts an integer value into a 4 byte (32bit) long byte array.
     *
     * @param i
     *         The integer value to be converted to a byte array
     *
     * @return The 4-byte array representing the integer value
     */
    public static byte[] int2bytes(int i)
    {
        byte[] result = new byte[4];

        result[0] = (byte) (i >> 24);
        result[1] = (byte) (i >> 16);
        result[2] = (byte) (i >> 8);
        result[3] = (byte) (i /* >> 0 */);
        return result;
    }

    /**
     * Converts a 4 byte long byte array into an integer value
     *
     * @param b
     *         The 4 byte array containing the bits of the integer
     *
     * @return The converted integer object
     */
    public static int bytes2int(byte[] b)
    {
        return new BigInteger(b).intValue();
    }

    /**
     * Converts a 8 byte long byte array into a long value
     *
     * @param b
     *         the 8 byte array containing the bits of the long value
     *
     * @return the converted long object
     */
    public static long byte2long(byte[] b)
    {
        return new BigInteger(b).longValue();
    }

    /**
     * Prints the content of the backing data store to the log file.
     *
     * @param name
     *         The name of the backing data store
     * @param keys
     *         The keys of the objects to print to the log file
     * @param valueClass
     *         The data type of the value object associated to the key
     *
     * @throws IOException
     *         If any error during reading the data store occurs
     * @throws DrumException
     *         Thrown if the next entry from the data store could not be extracted
     */
    public static <V extends ByteSerializer<V>> void printCacheContent(String name, List<Long> keys,
                                                                       Class<V> valueClass)
            throws IOException, DrumException
    {
        LOG.info("Data contained in cache.db:");

        RandomAccessFile cacheFile = DrumUtils.openDataStore(name);
        cacheFile.seek(0);

        Pair<Long, V> data;
        do
        {
            data = DrumUtils.getNextEntry(cacheFile, valueClass);
            if (data != null)
            {
                keys.add(data.getFirst());
                V hostData = data.getLast();
                if (hostData != null)
                {
                    LOG.info("Key: {}, Value: {}", data.getFirst(), hostData);
                }
                else
                {
                    LOG.info("Key: {}, Value: {}", data.getFirst(), null);
                }
            }
        }
        while (data != null);

        cacheFile.close();
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
     *
     * @return A key/value tuple
     *
     * @throws DrumException
     *         Thrown if either an IOException occurs during fetching the next Entry or the data can't be deserialized
     *         from bytes to an actual object
     */
    @SuppressWarnings("unchecked")
    public static <V extends ByteSerializer<V>> Pair<Long, V> getNextEntry(RandomAccessFile cacheFile,
                                                                           Class<? super V> valueClass)
            throws DrumException
    {
        // Retrieve the key from the file
        try
        {
            if (cacheFile.getFilePointer() == cacheFile.length())
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
                V value = null;
                // as we have our own serialization mechanism, we have to ensure
                // that these objects are serialized appropriately
                if (ByteSerializer.class.isAssignableFrom(valueClass))
                {
                    try
                    {
                        value = ((V) valueClass.newInstance());
                    }
                    catch (IllegalAccessException | InstantiationException e)
                    {
                        e.printStackTrace();
                    }
                    if (value != null)
                    {
                        value = value.readBytes(byteValue);
                    }
                    else
                    {
                        throw new DrumException("Could not read next entry as value was null before reading its bytes");
                    }
                }
                // should not happen - but in case we refactor again leave it in
                else
                {
                    value = DrumUtils.deserialize(byteValue, valueClass);
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
