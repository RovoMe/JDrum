package at.rovo.caching.drum.data;

/**
 * Convenience class to serialize and de-serialize {@link String} objects.
 *
 * @author Roman Vottner
 */
public class StringSerializer implements AppendableData<StringSerializer>
{
	/** The string object to serialize or de-serialize **/
	private String data = null;

	/**
	 * Creates a new instance with an empty String.
	 */
	public StringSerializer()
	{
		this.data = "";
	}

	/**
	 * Creates a new instance with the provided String.
	 *
	 * @param data
	 * 		The string to be used for serialization
	 */
	public StringSerializer(String data)
	{
		this.data = data;
	}

	/**
	 * Returns the deserialized string.
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
		{
			return this.data.getBytes();
		}
		return null;
	}

	@Override
	public StringSerializer readBytes(byte[] data)
	{
		return new StringSerializer(new String(data));
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
			{
				return true;
			}
		}
		return false;
	}

	@Override
	public String toString()
	{
		return this.data;
	}
}
