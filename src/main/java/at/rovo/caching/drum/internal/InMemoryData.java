package at.rovo.caching.drum.internal;

import at.rovo.caching.drum.DrumOperation;
import at.rovo.caching.drum.DrumResult;
import at.rovo.caching.drum.NotAppendableException;
import at.rovo.caching.drum.data.AppendableData;
import at.rovo.caching.drum.data.ByteSerializer;
import at.rovo.caching.drum.util.DrumUtil;

/**
 * <em>InMemoryData</em> is a bean which holds the data related to an object that either should be stored within the
 * DRUM cache or is returned due to a DUPLICATE event from a back-end data store.
 * <p>
 * Besides getter and setter methods for the corresponding fields it provides a convenience method to restore the
 * original ordering after a sorting through {@link #getPosition()}.
 * <p>
 * Moreover, the bytes of the key, value or auxiliary data attached to a key can be retrieved via {@link #getKey()},
 * {@link #getValue()} or {@link #getAuxiliary()} methods.
 *
 * @param <V>
 * 		The type of the value
 * @param <A>
 * 		The type of the auxiliary data attached to a key
 *
 * @author Roman Vottner
 */
public class InMemoryData<V extends ByteSerializer<V>, A extends ByteSerializer<A>>
{
	/** The key of the statement **/
	private Long key;
	/** The value belonging to the key **/
	private V value;
	/** Additional information related to the key **/
	private A aux;
	/** The DRUM operation to execute on the data **/
	private DrumOperation operation;
	/**
	 * Will keep track of the original position of this data object before sorting to revert the sorting after the
	 * key/value data was persisted in the bucket file
	 **/
	private int position = 0;
	/**
	 * The result extracted based on the data and the executed DRUM operation
	 **/
	private DrumResult result = null;

	/**
	 * Creates a new instance of this class without instantiating any field.
	 */
	public InMemoryData()
	{

	}

	/**
	 * Creates a new instance of this class and initializes required fields.
	 *
	 * @param key
	 * 		The key of this data object
	 * @param value
	 * 		The value of this data object
	 * @param aux
	 * 		The auxiliary data attached to the key
	 * @param operation
	 * 		The DRUM operation to execute for this data object
	 */
	public InMemoryData(Long key, V value, A aux, DrumOperation operation)
	{
		this.key = key;
		this.value = value;
		this.aux = aux;
		this.operation = operation;
	}

	/**
	 * Returns the key of the current data object.
	 *
	 * @return The key of the data object
	 */
	public Long getKey()
	{
		return this.key;
	}

	/**
	 * Sets the key of the current data object
	 *
	 * @param key
	 * 		The new key of the data object
	 */
	public void setKey(Long key)
	{
		this.key = key;
	}

	/**
	 * Returns the value of the current data object.
	 *
	 * @return The value object related to the key of this data object
	 */
	public V getValue()
	{
		return this.value;
	}

	/**
	 * Sets the value of the current data object.
	 *
	 * @param value
	 * 		The new value of the data object
	 */
	public void setValue(V value)
	{
		this.value = value;
	}

	/**
	 * Returns the additional information attached to a key for the current data object.
	 *
	 * @return The additional information of the data object
	 */
	public A getAuxiliary()
	{
		return this.aux;
	}

	/**
	 * Sets the additional information attached to a key for the current data object.
	 *
	 * @param aux
	 * 		The additional information of the data object
	 */
	public void setAuxiliary(A aux)
	{
		this.aux = aux;
	}

	/**
	 * Returns the DRUM operation to be used on this data object.
	 *
	 * @return The DRUM operation to be used
	 */
	public DrumOperation getOperation()
	{
		return this.operation;
	}

	/**
	 * Sets the DRUM operation to be used on this data object.
	 *
	 * @param operation
	 * 		The DRUM operation to be used
	 */
	public void setOperation(DrumOperation operation)
	{
		this.operation = operation;
	}

	/**
	 * Appends the value of the provided data object to the value of this data object.
	 *
	 * @param data
	 * 		The data object containing the value to append to the value of this data object
	 *
	 * @throws NotAppendableException
	 * 		Thrown if the data object to append is not an instance of {@link AppendableData}
	 */
	@SuppressWarnings("unchecked")
	public void appendValue(V data) throws NotAppendableException
	{
		if (this.value != null && this.value instanceof AppendableData)
		{
			((AppendableData<V>) this.value).append(data);
		}
		else
		{
			throw new NotAppendableException(
					"Value data does not implement " + "at.rovo.caching.drum.AppendableData interface!");
		}
	}

	/**
	 * Returns the key of this data object as an 8 byte long array in big-endian order.
	 *
	 * @return The key of this data object as byte-array
	 */
	public byte[] getKeyAsBytes()
	{
		return DrumUtil.long2bytes(key);
	}

	/**
	 * Returns the value of this data object as a byte-array in big endian order.
	 *
	 * @return The value of this data object as byte-array
	 */
	public byte[] getValueAsBytes()
	{
		if (this.value == null)
		{
			return null;
		}

		return this.value.toBytes();
	}

	/**
	 * Returns the attached data to a key for this data object as a byte-array in big endian order.
	 *
	 * @return The attached data to a key as byte-array
	 */
	public byte[] getAuxiliaryAsBytes()
	{
		if (this.aux == null)
		{
			return null;
		}

		return this.aux.toBytes();
	}

	/**
	 * Intended to store the original position in the array managed by {@link DiskFileMerger} to enable reverting the
	 * sorting after storing the key/value pair into the bucket file.
	 *
	 * @param position
	 * 		The original position for this data object before sorting
	 */
	public void setPosition(int position)
	{
		this.position = position;
	}

	/**
	 * Returns the original position of this data object before the sorting happened in the <em>doMerge()</em> method of
	 * {@link DiskFileMerger}.
	 *
	 * @return The original position of this data object
	 */
	public int getPosition()
	{
		return this.position;
	}

	/**
	 * Sets the result for the defined DRUM operation based on the data of this object.
	 *
	 * @param result
	 * 		The result for this data object
	 */
	public void setResult(DrumResult result)
	{
		this.result = result;
	}

	/**
	 * Returns the result gathered by DRUM based on the provided data object.
	 *
	 * @return The result for this data object
	 */
	public DrumResult getResult()
	{
		return this.result;
	}

	/**
	 * Returns the length of the key and the value in the byte-array.
	 *
	 * @return The length of the key and value in the byte-array
	 */
	public long getByteLengthKV()
	{
		// the 8 byte long key and the 4 byte long integer value representing
		// the length of bytes the value field does take
		long bytes = 12;
		if (this.value != null)
		{
			bytes += this.getValueAsBytes().length;
		}

		return bytes;
	}

	@Override
	public String toString()
	{
		return "op: " + this.operation + " key: " + this.key + " value: " + this.value + " aux: " + this.aux +
			   " result: " + this.result;
	}
}
