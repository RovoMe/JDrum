package at.rovo.caching.drum.event;

public class InMemoryBufferEvent extends DrumEvent<InMemoryBufferEvent>
{
	private int bucketId = 0;
	private long kvSize = 0L;
	private long auxSize = 0L;
	
	public InMemoryBufferEvent(String drumName, int bucketId, long kvSize, long auxSize)
	{
		super (drumName, InMemoryBufferEvent.class);
		this.bucketId = bucketId;
		this.kvSize = kvSize;
		this.auxSize = auxSize;
	}
	
	public int getBucketId()
	{
		return this.bucketId;
	}
	
	public long getKVSize()
	{
		return this.kvSize;
	}
	
	public long getAuxSize()
	{
		return this.auxSize;
	}
	
	@Override
	public String toString()
	{
		StringBuilder buffer = new StringBuilder();
		buffer.append(this.drumName);
		buffer.append(" - ");
		buffer.append(this.currentThread.getName());
		buffer.append(" - InMemoryBuffer ");
		buffer.append(this.bucketId);
		buffer.append(" new buffer size ");
		buffer.append(this.kvSize);
		buffer.append(" bytes for key/value buffer and ");
		buffer.append(this.auxSize);
		buffer.append(" bytes for the aux buffer");
		
		return buffer.toString();
	}
}
