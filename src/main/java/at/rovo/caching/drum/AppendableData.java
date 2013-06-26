package at.rovo.caching.drum;

public interface AppendableData<T> extends ByteSerializer<T>
{
	public void append(T data);
}
