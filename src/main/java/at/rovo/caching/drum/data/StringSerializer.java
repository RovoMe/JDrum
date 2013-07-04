package at.rovo.caching.drum.data;

/**
 * <p>
 * Convenience class to serialize and de-serialize {@link String} objects.
 * </p>
 * 
 * @author Roman Vottner
 * 
 */
public class StringSerializer implements AppendableData<StringSerializer>
{
	/** The string object to serialize or de-serialize **/
	private String data = null;

	/**
	 * <p>
	 * Creates a new instance with an empty String.
	 * </p>
	 */
	public StringSerializer()
	{
		this.data = "";
	}

	/**
	 * <p>
	 * Creates a new instance with the provided String.
	 * </p>
	 * 
	 * @param data
	 *            The string to be used for serialization
	 */
	public StringSerializer(String data)
	{
		this.data = data;
	}

	/**
	 * <p>
	 * Returns the deserialized string.
	 * </p>
	 * 
	 * @return The deserialized string
	 */
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
		this.data = this.data + data;
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
			if (((StringSerializer) o).getData().equals(this.data))
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
