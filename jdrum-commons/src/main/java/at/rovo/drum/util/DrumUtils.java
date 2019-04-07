package at.rovo.drum.util;

import at.rovo.drum.data.ByteSerializable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;

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
public class DrumUtils {

    /**
     * Calculates a 8-byte (64bit) hash from a provided input {@link String}.
     *
     * @param string The string object to generate the 64bit hash value for
     * @return The 64bit long hash value for the provided string
     */
    public static long hash(final String string) {
        long h = 1125899906842597L; // prime
        int len = string.length();

        for (int i = 0; i < len; i++) {
            h = 31 * h + string.charAt(i);
        }
        return h;
    }

    /**
     * Calculates a 8-byte (64bit) hash from an object by invoking the objects {@link Object#toString()} first and then
     * generating the hash value for the generated string value.
     *
     * @param object The object whose key shall be calculated
     * @return The 64bit long hash value for the provided object
     */
    public static long hash(final Object object) {
        return hash(object.toString());
    }

    /**
     * Calculates the bucket id the key should be placed in according to the first n bits of the key where n is the
     * power of 2 numBuckets is defined as. F.e if 1024 buckets are defined the first 10 bits of the key will be taken
     * as the index for the actual bucket (2^10 = 1024, 2^9 = 512, ...)
     *
     * @param key        The 64bit key whose bucket index should be calculated
     * @param numBuckets The total number of available buckets. This should be a power of two (e.g. 2, 4, 8, 16, 32, ...)
     * @return The bucket index the key should be in
     * @throws IllegalArgumentException If the provided input parameter is not a power of 2
     */
    public static int getBucketForKey(long key, int numBuckets) {
        // test if numBuckets is a power of 2
        int exponent = Math.getExponent(numBuckets);
        if (numBuckets != Math.pow(2, exponent)) {
            throw new IllegalArgumentException("Number of buckets does not correspond to a power of 2!");
        }
        return (int) (key >> (64 - exponent)) + numBuckets / 2;
    }

    /**
     * Calculates the bucket based on a 64bit key.
     * <p>
     * The calculation is based on an example presented by Leandro T C Melo
     *
     * @param key        The key whose bucket should be calculated
     * @param numBuckets The total number of available buckets. This should be a power of two (e.g. 2, 4, 8, 16, 32, ...)
     * @return The bucket the key should be in
     * @throws IllegalArgumentException If the provided input parameter is not a power of 2
     * @see <a href="http://www.codeproject.com/Articles/36221/DRUM-A-C-Implementation-for-the-URL-seen-Test-of-a">
     *     DRUM - A C++ Implementation for the URL-seen Test of a Web Crawler</a>
     */
    public static long getBucketOfKey(long key, int numBuckets) {
        int exponent = Math.getExponent(numBuckets);
        // Build mask-string
        StringBuilder builder = checkExponentAndPrepareMaskString(numBuckets, exponent);
        for (int i = 0; i < 64; i++) {
            builder.append(i < exponent ? "1" : "0");
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
     * @param key        The key whose bucket should be calculated
     * @param numBuckets The total number of available buckets. This should be a power of two (e.g. 2, 4, 8, 16, 32, ...)
     * @return The bucket the key should be in
     * @throws IllegalArgumentException If the provided input parameter is not a power of 2
     * @see <a href="http://www.codeproject.com/Articles/36221/DRUM-A-C-Implementation-for-the-URL-seen-Test-of-a">
     *     DRUM - A C++ Implementation for the URL-seen Test of a Web Crawler</a>
     */
    public static int getBucketOfKey(int key, int numBuckets) {
        int exponent = Math.getExponent(numBuckets);
        // Build mask-string
        StringBuilder builder = checkExponentAndPrepareMaskString(numBuckets, exponent);
        for (int i = 0; i < 32; i++) {
            builder.append(i < exponent ? "1" : "0");
        }
        int mask = new BigInteger(builder.toString(), 2).intValue();
        int bucket = mask & key;
        return (bucket >>> (32 - exponent));
    }

    private static StringBuilder checkExponentAndPrepareMaskString(int numBuckets, int exponent) {
        if (numBuckets != Math.pow(2, exponent)) {
            throw new IllegalArgumentException("Number of buckets does not correspond to a power of 2!");
        }
        // Build mask-string
        return new StringBuilder();
    }

    /**
     * Serializes an object into a byte-array.
     *
     * @param obj The object to convert into a binary array
     * @return The bytes of the object or an empty byte array if the source object was null
     * @throws IOException If any error during the serialization occurs
     */
    public static byte[] serialize(Object obj) throws IOException {
        if (obj instanceof String) {
            return obj.toString().getBytes(StandardCharsets.UTF_8);
        }
        // objects implementing the ByteSerializable interface do their own homework on serializing/deserializing their
        // state to/from bytes. This avoids returning unnecessary bytes
        if (obj instanceof ByteSerializable) {
            return ((ByteSerializable) obj).toBytes();
        }
        // no custom serialization available, fall back to the default one although it is wasteful in relation to the
        // number of actual bytes produced! Simple Integer objects create up to 81 bytes instead of only 4!
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            if (obj == null) {
                return new byte[0];
            }
            oos.writeObject(obj);
            oos.flush();

            return baos.toByteArray();
        }
    }

    /**
     * Turns a byte-array into an object
     *
     * @param bytes The bytes of the object
     * @param type The class for which the provided <em>bytes</em> should be transformed into a new instance for
     * @param <V> The type of the value to deserialize into
     * @return The object deserialized from the array of bytes provided
     * @throws IOException If the cast of the bytes to a {@link ByteSerializable} object failed
     * @throws ClassNotFoundException If the class requested via the <em>type</em> parameter could not be found
     */
    public static <V> V deserialize(byte[] bytes, Class<? extends V> type)
            throws IOException, ClassNotFoundException {
        V ret;
        // check if the byte array is a String (character array)
        if (String.class.isAssignableFrom(type)) {
            ret = type.cast(new String(bytes, StandardCharsets.UTF_8));
        } else if (ByteSerializable.class.isAssignableFrom(type)) {
            try {
                ret = type.cast(((ByteSerializable) type.getConstructor().newInstance()).readBytes(bytes));
            } catch (Exception ex) {
                throw new IOException(ex);
            }
        } else {
            try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
                 ObjectInputStream ois = new ObjectInputStream(bais)) {
                Object obj = ois.readObject();
                ret = type.cast(obj);
            }
        }
        return ret;
    }

    /**
     * Converts a long value into a 8 byte (64bit) long byte array.
     *
     * @param l The long value to be converted into a byte array
     * @return the 8-byte array representing the long value
     */
    public static byte[] long2bytes(long l) {
        byte[] result = new byte[8];

        for (int i = 0; i < result.length; i++) {
            result[i] = (byte) (l >> ((8 * result.length) - (8 * (i + 1))));
        }
        return result;
    }

    /**
     * Converts an integer value into a 4 byte (32bit) long byte array.
     *
     * @param i The integer value to be converted to a byte array
     * @return The 4-byte array representing the integer value
     */
    public static byte[] int2bytes(int i) {
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
     * @param b The 4 byte array containing the bits of the integer
     * @return The converted integer object
     */
    public static int bytes2int(byte[] b) {
        return new BigInteger(b).intValue();
    }

    /**
     * Converts a 8 byte long byte array into a long value
     *
     * @param b the 8 byte array containing the bits of the long value
     * @return the converted long object
     */
    public static long byte2long(byte[] b) {
        return new BigInteger(b).longValue();
    }
}
