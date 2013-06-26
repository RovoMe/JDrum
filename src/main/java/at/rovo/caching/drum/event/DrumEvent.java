package at.rovo.caching.drum.event;

public abstract class DrumEvent<T extends DrumEvent<T>>
{
	protected String drumName = null;
	protected Class<T> clazz = null;
	protected final Thread currentThread = Thread.currentThread();
	
	DrumEvent(String drumName, Class<T> clazz)
	{
		this.drumName = drumName;
		this.clazz = clazz;
	}
	
	public String getDrumName()
	{
		return this.drumName;
	}
	
	public Class<T> getRealClass()
	{
		return this.clazz;
	}
	
	public Thread getThread()
	{
		return this.currentThread;
	}
}
