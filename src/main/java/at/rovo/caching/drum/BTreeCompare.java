package at.rovo.caching.drum;

import java.io.Serializable;
import java.util.Comparator;

public class BTreeCompare implements Comparator<byte[]>, Serializable
{
	private static final long serialVersionUID = -8852688978869943639L;

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
