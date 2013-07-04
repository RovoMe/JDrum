package at.rovo.caching.drum.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.StreamCorruptedException;
import java.math.BigInteger;
import java.util.Arrays;

/**
 * <p>
 * This utility class provides some basic methods to convert 32bit integer
 * values to a byte-array and vice versa and to extract 64bit hash codes from
 * String objects and get the number of a bucket based on a key and the number
 * of buckets for 32- and 64bit calculations.
 * </p>
 * <p>
 * Note however, that Java only supports lists with 32bit integer length -
 * therefore the 64bit versions here aren't used in the application itself.
 * </p>
 * 
 * @author Roman Vottner
 */
public class DrumUtil
{
	/**
	 * <p>
	 * Calculates a 8-byte (64bit) hash from a {@link String}
	 * </p>
	 * 
	 * @param string
	 * @return
	 */
	public final static long hash(final String string)
	{
		long h = 1125899906842597L; // prime
		int len = string.length();

		for (int i = 0; i < len; i++)
			h = 31 * h + string.charAt(i);
		return h;
	}

	/**
	 * <p>
	 * Calculates a 8-byte (64bit) hash from an object
	 * </p>
	 * 
	 * @param object
	 *            The object whose key shall be calculated
	 * @return
	 */
	public final static long hash(final Object object)
	{
		return hash(object.toString());
	}

	/**
	 * <p>
	 * Calculates the bucket id the key should be placed in according to the
	 * first n bits of the key where n is the power of 2 numBuckets is defined
	 * as. F.e if 1024 buckets are defined the first 10 bits of the key will be
	 * taken as the index for the actual bucket (2^10 = 1024, 2^9 = 512, ...)
	 * </p>
	 * 
	 * @param key
	 *            The 64bit key whose bucket index should be calculated
	 * @param numBuckets
	 *            The total number of available buckets. This should be a power
	 *            of two (e.g. 2, 4, 8, 16, 32, ...)
	 * @return The bucket index the key should be in
	 */
	public final static int getBucketForKey(long key, final int numBuckets)
	{
		// test if numBuckets is a power of 2
		int exponent = Math.getExponent(numBuckets);
		if (numBuckets != Math.pow(2, exponent))
			throw new IllegalArgumentException(
					"Number of buckets does not correspond to a power of 2!");
		return (int) (key >> (64 - exponent)) + numBuckets / 2;
	}

	/**
	 * <p>
	 * Calculates the bucket based on a 64bit key.
	 * </p>
	 * <p>
	 * The calculation is based on an example presented by Leandro T C Melo
	 * </p>
	 * 
	 * @param key
	 *            The key whose bucket should be calculated
	 * @param numBuckets
	 *            The total number of available buckets. This should be a power
	 *            of two (e.g. 2, 4, 8, 16, 32, ...)
	 * @return The bucket the key should be in
	 * @see http://www.codeproject.com/Articles/36221/DRUM-A-C-Implementation-for-the-URL-seen-Test-of-a
	 */
	public final static long getBucketOfKey(final long key, final int numBuckets)
	{
		// test if numBuckets is a power of 2
		int exponent = Math.getExponent(numBuckets);
		if (numBuckets != Math.pow(2, exponent))
			throw new IllegalArgumentException(
					"Number of buckets does not correspond to a power of 2!");
		// Build mask-string
		StringBuilder builder = new StringBuilder();
		for (int i = 0; i < 64; i++)
		{
			if (i < exponent)
				builder.append("1");
			else
				builder.append("0");
		}
		long mask = new BigInteger(builder.toString(), 2).longValue();
		long bucket = mask & key;
		return (bucket >>> (64 - exponent));
	}

	/**
	 * <p>
	 * Calculates the bucket based on a 32bit key.
	 * </p>
	 * <p>
	 * The calculation is based on an example presented by Leandro T C Melo
	 * </p>
	 * 
	 * @param key
	 *            The key whose bucket should be calculated
	 * @param numBuckets
	 *            The total number of available buckets. This should be a power
	 *            of two (e.g. 2, 4, 8, 16, 32, ...)
	 * @return The bucket the key should be in
	 * @see http://www.codeproject.com/Articles/36221/DRUM-A-C-Implementation-for-the-URL-seen-Test-of-a
	 */
	public final static int getBucketOfKey(final int key, final int numBuckets)
	{
		// test if numBuckets is a power of 2
		int exponent = Math.getExponent(numBuckets);
		if (numBuckets != Math.pow(2, exponent))
			throw new IllegalArgumentException(
					"Number of buckets does not correspond to a power of 2!");
		// Build mask-string
		StringBuilder builder = new StringBuilder();
		for (int i = 0; i < 32; i++)
		{
			if (i < exponent)
				builder.append("1");
			else
				builder.append("0");
		}
		int mask = new BigInteger(builder.toString(), 2).intValue();
		int bucket = mask & key;
		return (bucket >>> (32 - exponent));
	}

	/**
	 * <p>
	 * Serializes an object into a byte-array
	 * </p>
	 * 
	 * @param obj
	 *            The object to convert into a binary array
	 * @return The bytes of the object or an empty byte array if the source
	 *         object was null
	 * @throws IOException
	 *             If any error during the serialization occurs
	 */
	public synchronized static byte[] serialize(Object obj) throws IOException
	{
		if (obj instanceof String)
			return new String(obj.toString()).getBytes();

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
		baos = null;
		oos = null;

		return bytes;
	}

	/**
	 * <p>
	 * Turns a byte-array into an object
	 * </p>
	 * 
	 * @param bytes
	 *            The bytes of the object
	 * @return The object deserialized from the array of bytes provided
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	public static <T> T deserialize(byte[] bytes, Class<T> type)
			throws IOException, ClassNotFoundException
	{
		T ret = null;
		// check if the byte array is a String (character array)
		if (type.isAssignableFrom(String.class))
		{
			ret = type.cast(new String(bytes));
			bytes = null;
		}
		else
		{
			try
			{
				ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
				ObjectInputStream ois = new ObjectInputStream(bais);
				// ois.mark(0);
				Object obj = ois.readObject();
				ret = type.cast(obj);

				if (ois.markSupported())
					ois.reset();
				ois.close();
				ois = null;

				if (bais.markSupported())
					bais.reset();
				bais.close();
				bais = null;
			}
			catch (StreamCorruptedException e)
			{
				System.err.println("Error deserializing object of type "
						+ type.getCanonicalName() + " for bytes: " + bytes
						+ "! Reason: " + e.getLocalizedMessage());
				throw e;
			}
		}
		return ret;
	}

	/**
	 * <p>
	 * Converts a long value into a 8 byte (64bit) long byte array.
	 * </p>
	 * 
	 * @param l
	 *            The long value to be converted into a byte array
	 * @return the 8-byte array representing the long value
	 */
	public static byte[] long2bytes(long l)
	{
		byte[] result = new byte[8];

		for (int i = 0; i < result.length; i++)
			result[i] = (byte) (l >> ((8 * result.length) - (8 * (i + 1))));
		return result;
	}

	/**
	 * <p>
	 * Converts an integer value into a 4 byte (32bit) long byte array.
	 * </p>
	 * 
	 * @param i
	 *            The integer value to be converted to a byte array
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
	 * <p>
	 * Converts a 4 byte long byte array into an integer value
	 * </p>
	 * 
	 * @param b
	 *            The 4 byte array containing the bits of the integer
	 * @return The converted integer object
	 */
	public static int bytes2int(byte[] b)
	{
		return new BigInteger(b).intValue();
	}

	/**
	 * <p>
	 * Converts a 8 byte long byte array into a long value
	 * </p>
	 * 
	 * @param b
	 *            the 8 byte array containing the bits of the long value
	 * @return the converted long object
	 */
	public static long byte2long(byte[] b)
	{
		return new BigInteger(b).longValue();
	}

}
