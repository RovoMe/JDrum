package at.rovo.caching.drum.event;

public class MergerStateUpdate extends DrumEvent<MergerStateUpdate>
{
	private MergerState state = null;
	private Integer bucketId = null;
	
	public MergerStateUpdate(String drumName, MergerState state)
	{
		super(drumName, MergerStateUpdate.class);
		this.state = state;
	}
	
	public MergerStateUpdate(String drumName, MergerState state, int bucketId)
	{
		super(drumName, MergerStateUpdate.class);
		this.state = state;
		this.bucketId = bucketId;
	}
	
	public MergerState getState()
	{
		return this.state;
	}
	
	public Integer getBucketId()
	{
		return this.bucketId;
	}
	
	@Override
	public String toString()
	{
		StringBuilder buffer = new StringBuilder();
		buffer.append(this.drumName);
		buffer.append(" - ");
		buffer.append(this.currentThread.getName());
		buffer.append(" - Merger state changed to: ");
		buffer.append(this.state);
		
		return buffer.toString();
	}
}
