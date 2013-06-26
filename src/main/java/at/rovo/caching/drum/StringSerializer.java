package at.rovo.caching.drum;

public class StringSerializer implements AppendableData<StringSerializer>
{
	private String data = null;
	
	public StringSerializer()
	{
		this.data = "";
	}
	
	public StringSerializer(String data)
	{
		this.data = data;
	}
	
	public String getData()
	{
		return this.data;
	}
	
	@Override
	public byte[] toBytes()
	{
		if (this.data != null)
			return this.data.getBytes();
		return null;
	}

	@Override
	public StringSerializer readBytes(byte[] data)
	{
		StringSerializer obj = new StringSerializer(new String(data));
		return obj;
	}

	@Override
	public void append(StringSerializer data)
	{
		this.data = this.data+data;		
	}
	
	@Override
	public int hashCode()
	{
		return this.data.hashCode();
	}
	
	@Override
	public boolean equals(Object o)
	{
		if (o instanceof StringSerializer)
		{
			if (((StringSerializer)o).getData().equals(this.data))
				return true;
		}
		return false;
	}
	
	@Override
	public String toString()
	{
		return this.data;
	}
}
