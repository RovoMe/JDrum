package at.rovo.caching.drum;

public interface ByteSerializer<T>
{
	public byte[] toBytes();
	public T readBytes(byte[] data);
}
