package at.rovo.caching.drum.data;

/**
 * <p>
 * Convenience class to support serializing and de-serialization of null
 * elements.
 * </p>
 * 
 * @author Roman Vottner
 */
public class NullSerializer implements ByteSerializer<NullSerializer>
{
	/**
	 * <p>
	 * Instantiates a new instance which represents a null value.
	 * </p>
	 */
	public NullSerializer()
	{

	}

	@Override
	public byte[] toBytes()
	{
		byte[] bytes = new byte[0];
		return bytes;
	}

	@Override
	public NullSerializer readBytes(byte[] data)
	{
		return new NullSerializer();
	}
}
