package at.rovo.caching.drum;

public class NullSerializer implements ByteSerializer<NullSerializer>
{
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
