package at.rovo.caching.drum.internal.backend.berkeley;

import java.io.Serializable;
import java.util.Comparator;
import at.rovo.caching.drum.util.DrumUtil;

/**
 * <p>
 * Compares an 8 byte sequence representing a key stored in a Berkeley DB with
 * the 8 byte sequence representing the key of a new data object which should be
 * either checked for equality or inserted.
 * </p>
 * <p>
 * The comparison itself is done on the long value of the byte representation.
 * </p>
 * 
 * @author Roman Vottner
 */
public class BTreeCompare implements Comparator<byte[]>, Serializable
{
	/** Unique serialization ID **/
	private static final long serialVersionUID = -8852688978869943639L;

	@Override
	public int compare(byte[] bytes1, byte[] bytes2)
	{
		if (bytes1 == null && bytes2 == null)
			return 0;

		long i1 = DrumUtil.byte2long(bytes1);
		long i2 = DrumUtil.byte2long(bytes2);

		if (i1 < i2)
			return -1;
		else if (i1 > i2)
			return 1;
		return 0;
	}

}
