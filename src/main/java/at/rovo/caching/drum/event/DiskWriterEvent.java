package at.rovo.caching.drum.event;

public class DiskWriterEvent extends DrumEvent<DiskWriterEvent>
{
	private int bucketId = 0;
	private long kvBytes = 0L;
	private long auxBytes = 0L;

	
	public DiskWriterEvent(String drumName, int bucketId, long kvBytes, long auxBytes)
	{
		super(drumName, DiskWriterEvent.class);
		this.bucketId = bucketId;
		this.kvBytes = kvBytes;
		this.auxBytes = auxBytes;
	}
	
	public int getBucketId()
	{
		return this.bucketId;
	}
	
	public long getKVBytes()
	{
		return this.kvBytes;
	}
	
	public long getAuxBytes()
	{
		return this.auxBytes;
	}
	
	@Override
	public String toString()
	{
		StringBuilder buffer = new StringBuilder();
		buffer.append(this.drumName);
		buffer.append(" - ");
		buffer.append(this.currentThread.getName());
		buffer.append(" - DiskWriter ");
		buffer.append(this.bucketId);
		buffer.append(" has written ");
		buffer.append(this.kvBytes);
		buffer.append(" bytes into bucket");
		buffer.append(this.bucketId);
		buffer.append(".kv file and ");
		buffer.append(this.auxBytes);
		buffer.append(" bytes into bucket");
		buffer.append(this.bucketId);
		buffer.append(".aux file");
		
		return buffer.toString();
	}
}
