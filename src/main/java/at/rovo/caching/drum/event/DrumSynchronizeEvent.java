package at.rovo.caching.drum.event;

public class DrumSynchronizeEvent extends DrumEvent<DrumSynchronizeEvent>
{

	public DrumSynchronizeEvent(String drumName)
	{
		super(drumName, DrumSynchronizeEvent.class);
	}

	public String toString()
	{
		StringBuffer buffer = new StringBuffer();
		buffer.append(this.drumName);
		buffer.append(" - ");
		buffer.append(this.currentThread.getName());
		buffer.append(" - DRUM synchronization invoked");
		
		return buffer.toString();
	}
}
