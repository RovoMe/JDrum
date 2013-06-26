package at.rovo.caching.drum;

public class InMemoryData<V extends ByteSerializer<V>, A extends ByteSerializer<A>>
{
	private Long key;
	private V value;
	private A aux;
	private DrumOperation operation;
	private int position = 0;
	private DrumResult result = null;
	
	public InMemoryData()
	{
		
	}
	
	public InMemoryData(Long key, V value, A aux, DrumOperation operation)
	{
		this.key = key;
		this.value = value;
		this.aux = aux;
		this.operation = operation;
	}
	
	public Long getKey() { return this.key; }
	public void setKey(Long key) { this.key = key; }
	
	public V getValue() { return this.value; }
	public void setValue(V value) { this.value = value; }
	
	
	public A getAuxiliary() { return this.aux; }
	public void setAuxiliary(A aux) { this.aux = aux; }
	
	public DrumOperation getOperation() { return this.operation; }
	public void setOperation(DrumOperation operation) { this.operation = operation; }
	
	public void appendValue(V data) throws NotAppendableException
	{
		if (this.value != null && this.value instanceof AppendableData)
		{
			((AppendableData<V>)this.value).append(data);
		}
		else
			throw new NotAppendableException("Value data does not implement at.rovo.caching.drum.AppendableData interface!");
	}
	
	public byte[] getKeyAsBytes() { return DrumUtil.long2bytes(key); }
	public byte[] getValueAsBytes() 
	{ 
		if (this.value == null)
			return null;

		return this.value.toBytes();
	}
	
	public byte[] getAuxiliaryAsBytes()
	{
		if (this.aux == null)
			return null;
		
		return this.aux.toBytes();
	}
	
	public void setPosition(int position) { this.position = position; }
	public int getPosition() { return this.position; }
	
	public void setResult(DrumResult result) { this.result  = result; }
	public DrumResult getResult() { return this.result; }
	
	public long getByteLengthKV() 
	{
		// the 8 byte long key and the 4 byte long integer value representing
		// the length of bytes the value field does take 
		long bytes = 12;
		if (this.value != null)
			bytes += this.getValueAsBytes().length;
		
		return bytes;
	}
	
	public String toString()
	{
		return "op: "+this.operation+" key: "+this.key+" value: "+this.value+" aux: "+this.aux+" result: "+this.result;
	}
}
