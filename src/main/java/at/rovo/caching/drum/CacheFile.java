package at.rovo.caching.drum;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * <p><code>CacheFile</code> is a key/value storage which enables updates of
 * existing key/value pairs.</p>
 * <p>This key/value store is tailored for the Disk Repository with Update
 * Management structure in that it enables a one-through pass processing of
 * the actual stored data. Therefore entries written to the store have to be
 * provided in a sorted order.</p>
 * 
 * @author Roman Vottner
 *
 * @param <V> The type of the value object associated to a key
 */
public class CacheFile<V extends ByteSerializer<V>>
{
	// create a logger
	private final static Logger logger = LogManager.getLogger(CacheFile.class.getName());
	/** The backing caching file to store data to and read it from **/
	private RandomAccessFile file = null;
	/** The name of the cache file**/
	private String name = null;
	/** The last key written to the data storage **/
	private Long lastKey = null;
	/** The size of the last entry **/
	private long entrySize = 0L;
	/** The last stored element **/
	private InMemoryData<V,?> lastElement = null;
	/** The name of the drum instance **/
	private String drum = null;
	/** The class-element of the value object **/
	private Class<V> valueClass = null;
	/** The number of entries in the cache file **/
	private long numEntries = 0L;

	/**
	 * <p>Creates a new instance and sets the name of the file to store data in
	 * to.</p>
	 * 
	 * @param name The name of the file to store data in
	 * @param drum The name of the drum instance the cache file is used for
	 * @param valueClass The class of the value object stored with the key
	 */
	public CacheFile(String name, String drum, Class<V> valueClass)
	{
		try 
		{
			this.name = name;
			this.drum = drum;
			this.file = new RandomAccessFile(this.name, "rw");
			this.valueClass = valueClass;
		} 
		catch (Exception e) 
		{	if (logger.isErrorEnabled())
				logger.error("["+this.drum+"] - Could not initialize backing cache file!", e);
			e.printStackTrace();
			throw new DrumException("Error initializing cache file! Caught reason: "+e.getLocalizedMessage(),e);
		}
	}
	
	/**
	 * <p>Returns the length of the data store in bytes.</p>
	 * 
	 * @return The length of the data store in bytes
	 */
	public long length()
	{
		if (this.file != null)
		{
			try
			{
				return this.file.length();
			}
			catch (Exception e)
			{
				if (logger.isErrorEnabled())
					logger.error("["+this.drum+"] - Error extracting file length of cache file!", e);
				e.printStackTrace();
				throw new DrumException("Error extracting file length of cache file! Caught reason: "+e.getLocalizedMessage(),e);
			}
		}
		return 0L;
	}
	
	/**
	 * <p>Returns the number of entries in the cache file.</p>
	 * 
	 * @return The number of entries in the cache file
	 */
	public long getNumberOfEntries()
	{
		return this.numEntries;
	}
	
	/**
	 * <p>Closes the cache file.</p>
	 */
	public void close()
	{
		if (this.file != null)
		{
			try
			{
				this.file.close();
			}
			catch (Exception e)
			{
				if (logger.isErrorEnabled())
					logger.error("["+this.drum+"] - Error closing cache file!", e);
				e.printStackTrace();
				throw new DrumException("Error closing cache file! Caught reason: "+e.getLocalizedMessage(),e);
			}
		}
	}
	
	/**
	 * <p>Resets the internal file pointer to the start of the file and clears
	 * cached data to simulate a new run-through.</p>
	 */
	public void reset()
	{
		this.lastKey = null;
		this.entrySize = 0L;
		if (this.file != null)
		{
			try
			{
				if (this.file.getChannel().isOpen())
					this.file.seek(0);
			}
			catch (Exception e)
			{
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * <p>Returns the entry starting at the current cursor position. If the
	 * cursor is in the middle of an entry it will not find any useful results!
	 * So make sure to set the cursor to the beginning of an entry first before
	 * invoking this method!</p>
	 * 
	 * @return The entry at the current cursor position or null if either the 
	 *         end of the file was reached without finding an entry or if the
	 *         cursor was placed in the middle of an entry.
	 * @throws IllegalAccessException 
	 * @throws InstantiationException 
	 */
	public <A extends ByteSerializer<A>> InMemoryData<V,?> getNextEntry() throws IOException, InstantiationException, IllegalAccessException
	{
		try
		{
			if (this.file.getFilePointer() == this.file.length())
				return null;
		
			Long key = file.readLong();
			if (logger.isDebugEnabled() && this.drum.equals("pldIndegree"))
				logger.debug("Reading key: "+key);
			
			// Retrieve the value from the file
			int valueSize = this.file.readInt();
			if (logger.isDebugEnabled() && this.drum.equals("pldIndegree"))
				logger.debug("value size: "+valueSize);
			if (valueSize > 0)
			{
				byte[] byteValue = new byte[valueSize];
				this.file.read(byteValue);
				if (logger.isDebugEnabled() && this.drum.equals("pldIndegree"))
					logger.debug("byte value: "+Arrays.toString(byteValue));
				V value = this.valueClass.newInstance().readBytes(byteValue);
				return new InMemoryData<V,A>(key, value, null, null);
			}
			return new InMemoryData<V,A>(key, null, null, null);
		}
		catch (InstantiationException | IllegalAccessException | IOException ex)
		{
			if (logger.isErrorEnabled())
				logger.error("["+this.drum+"] - Error fetching next entry from cache!", ex);
			throw ex;
		}
	}
	
	/**
	 * <p>Writes a new pair of key and value data into the cache file 
	 * in a sorted order depending on the value of the key.</p>
	 * <p>If a key with the same value exists, it will be overwritten to
	 * update the new value for this key. Existing entries located after
	 * the data to write will be moved further backwards.</p>
	 * 
	 * @param data The data to write into the cache file
	 * @return The updated entry
	 * @throws IOException
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 */
	public InMemoryData<V,?> writeEntry(InMemoryData<V,?> data) throws IOException, InstantiationException, IllegalAccessException
	{
		return this.writeEntry(data, false);
	}
	
	/**
	 * <p>Writes a new pair of key and value data into the cache file 
	 * in a sorted order depending on the value of the key.</p>
	 * <p>If a key with the same value exists, it will be overwritten to
	 * update the new value for this key. Existing entries located after
	 * the data to write will be moved further backwards.</p>
	 * 
	 * @param data The data to write into the cache file
	 * @param append Specifies if the data to write should be appended to an 
	 *               already existing entry with the same key.
	 * @return The updated entry
	 * @throws IOException
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 */
	public InMemoryData<V,?> writeEntry(InMemoryData<V,?> data, boolean append) throws IOException, InstantiationException, IllegalAccessException
	{
		if (logger.isDebugEnabled())
			logger.debug("["+this.drum+"] - writing entry: "+data.getKey()+"; value: "+data.getValue());
		
		/*
		 * Source: http://stackoverflow.com/questions/12677170/write-bytes-into-a-file-without-erasing-existing-bytes?lq=1
		 * 
		 * The only way to do this is to move the bytes that are currently in the 
		 * way. Depending on how frequently you have to do this, and how large 
		 * the file is, it's often a better idea to create a new file, copying 
		 * the existing file and inserting the new bytes along the way.
		 * 
		 * If you need to update the file infrequently, or it's small (up to 
		 * maybe 100kb) you can use a RandomAccessFile:
		 * 
		 * Extend the file, using the setLength() method, adding the number of 
		 * bytes you'll be inserting to whatever is returned by the length() 
		 * method.
		 * 
		 * If you have enough memory, create a byte[] that will hold all the 
		 * bytes from the insertion point to the previous end of file.
		 * 
		 * Call seek() to position at the insertion point
		 * Call readFully() to fill your temporary array
		 * Call seek() to position at the insertion point + the number of bytes to insert
		 * Call write() to write your buffer at that point
		 * Call seek() to reposition at the insertion point
		 * Call `write() to write the new bytes
		 * 
		 * If you can't create a large-enough array in step #2, you'll have to 
		 * perform steps 3-6 in a loop with a smaller buffer. I would use at 
		 * least a 64k buffer for efficiency.
		 */
		try 
		{
			long entryStartPosition = this.file.getFilePointer();
			
			// entry should be added to the end of the file
			if (entryStartPosition == this.file.length())
			{
				if ((this.lastKey != null && this.lastKey.equals(data.getKey())))
					this.updateLastEntry(data, append);
				else
				{
					this.addNewEntryAtTheEnd(data);
					this.numEntries++;
				}
				
				return this.lastElement;
			}
			// insert in the middle of the file
			else
			{
				// get the old length of the entry and calculate the area to shift
				InMemoryData<V,?> entry = null;
				long shiftBits = 0L;
				do
				{
					entry = this.getNextEntry();
					if (entry != null)
						shiftBits += entry.getByteLengthKV();
				}
				while ((entry != null && entry.getKey() < data.getKey()));
				
				// entry was not found so it is a new entry
				if (entry == null)
				{
					this.numEntries++;
					this.writeDataEntry(data, true);
					return this.lastElement;
				}
				
				// check if the new data should be integrated into the existing data entry
				// this results in an append instead of an replacement of the data entry
				if (append)
					data.appendValue(entry.getValue());
				
				// calculate the bytes to extend the file
				long byte2write = data.getByteLengthKV();
				if (data.getKey().equals(entry.getKey()))
					byte2write -= entry.getByteLengthKV();
				
				long restLength = this.file.length() - this.file.getFilePointer();
				// read the data to shift and store it into a temporary memory list
				final int segmentSize = 65536; // 64k buffer size
				// shift the content by the number of bytes to write
				this.shiftContent(byte2write, segmentSize, restLength);
				
				// set the cursor to the position where the new or updateable 
				// data should be written
				long pos = Math.max(entryStartPosition+shiftBits-entry.getByteLengthKV(), 0L);
				this.file.seek(pos);
				// write the new data
				this.writeDataEntry(data, true);
				
				// check if the entry is an update or an insert
				if (!data.getKey().equals(entry.getKey()))
				{
					this.numEntries++;
					// insert - add the old data after the new data
					this.writeDataEntry(entry, false);
				}
				
				// set the cursor back to the position we inserted the data
				// in case multiple elements for the same key are in the list
				this.file.seek(pos);		
				
				return this.lastElement;
			}
		} 
		catch (IOException | InstantiationException | IllegalAccessException e) 
		{
			if (logger.isErrorEnabled())
				logger.error("["+this.drum+"] - Error writing entry to cache!", e);
			throw e;
		}
		finally
		{
			if (logger.isDebugEnabled())
				this.printCacheContent(null, null);
		}
	}
	
	public InMemoryData<V,?> getEntry(Long key) throws IOException, InstantiationException, IllegalAccessException
	{
		if (logger.isDebugEnabled())
			logger.debug("["+this.drum+"] - Retrieving entry: "+key);
		try
		{
			long pos = this.file.getFilePointer();
						
			if (pos < this.file.length())
			{
				InMemoryData<V,?> data = null;
				do
				{
					data = this.getNextEntry();	
				}
				while (data != null && data.getKey() < key);
				
				// we haven't found a item till the end - so we can safely assume
				// that the key must be greater than any key stored within the
				// cache. The file pointer can therefore be left at the end of
				// the file
				if (data == null)
					return null;
				
				// set the pointer back to the object before the last read
				// item. We know that every further data item is greater and
				// new queries for a key either deal the last object we've
				// extracted or any object behind it - but not before
				this.file.seek(Math.max(0, this.file.getFilePointer()-data.getByteLengthKV()));
				// check if the key equals
				if (data.getKey().equals(key))
				{
					if (logger.isDebugEnabled())
						logger.debug("["+this.drum+"] - Found entry: "+data.getKey()+"; value: "+data.getValue());
					return data;
				}
				return null;
			}	
			else
			{
				// we have an entry at the end
				if (key.equals(this.lastKey))
				{
					return this.lastElement;
				}
			}
		}
		catch (IOException | InstantiationException | IllegalAccessException e)
		{
			if (logger.isErrorEnabled())
				logger.error("["+this.drum+"] - Exception while looking up key "+key+"! Reason: "+e.getLocalizedMessage(), e);
			throw e;
		}
		return null;
	}
	
	/**
	 * <p>Persists a data object to disk file</p>
	 * 
	 * @param data The object containing the data to write
	 * @param cacheEntry Defines if the data written should be temporarily 
	 *                   cached to compare it in the next iteration with the
	 *                   next entry
	 * @return The bytes actually written
	 * @throws IOException
	 */
	private long writeDataEntry(InMemoryData<V,?> data, boolean cacheEntry) throws IOException
	{
		long byte2write = data.getByteLengthKV();
		this.file.write(data.getKeyAsBytes());
		
		byte[] byteValue = data.getValueAsBytes();
		if (byteValue != null)
		{
			this.file.writeInt(byteValue.length);
			this.file.write(byteValue);
		}
		else
			this.file.writeInt(0);
		
		if (cacheEntry)
		{
			this.lastKey = data.getKey();
			this.entrySize = byte2write;
			this.lastElement = data;
		}
		
		return byte2write;
	}
	
	/**
	 * <p>Shifts the remaining content of a file by <code>byte2write</code> to 
	 * the right, creating a gap to write the new data into. With <code>
	 * segmentSize</code> the number of bytes read in one block can be defined
	 * to prevent {@link OutOfMemoryError}s due to filling up the heap-space
	 * of the current JVM instance.</p>
	 * 
	 * @param byte2write The number of bytes to extend the current file
	 * @param segmentSize The size of a segment to read in case the total file 
	 *                    length exceeds the segment size
	 * @param restLength The total number of bytes remaining to shift
	 * @throws IOException
	 */
	private void shiftContent(long byte2write, int segmentSize, long restLength) 
			throws IOException
	{
		// the rest of the file fits into one segment
		if (restLength < segmentSize)
		{
			long tmp = this.file.getFilePointer();
			byte[] rest = new byte[(int)restLength];
			// read the remaining content
			this.file.readFully(rest);
			// extend the file size by the bytes to write
			if (byte2write > 0)
				this.file.setLength(this.file.length()+byte2write);
			// set the cursor to the new position
			this.file.seek(tmp+byte2write);
			// and write the bytes
			this.file.write(rest);
			if (byte2write < 0)
				this.file.setLength(this.file.length()+byte2write);
			// set the cursor back to the origin position
			this.file.seek(tmp);
		}
		// we do have more than one segment to write
		else
		{
			// split the read up in to 64k parts
			long remainingBytes = restLength;
			// save the current position of the cursor
			long tmp = this.file.getFilePointer();
			// set the pointer to the end of the file
			long pos = this.file.length();
			// enlarge the file size by the bytes to write
			if (byte2write > 0)
				this.file.setLength(this.file.length()+byte2write);
			
			byte[] partBefore = null;
			boolean initial = true;
			do
			{
				remainingBytes -= segmentSize;
				if (byte2write < 0)
				{
					// extract the segment before the segment before as the end
					// would get overwritten
					int segment = 0;
					if (remainingBytes > segmentSize)
						segment = segmentSize;
					else
						segment = (int)remainingBytes;
					
					partBefore = new byte[segment];
					byte[] part = new byte[segmentSize];
					pos -= (segment + segmentSize);
					this.file.seek(Math.max(0, pos));
					// read the first segment
					this.file.read(partBefore, 0, partBefore.length);
					// read the actual segment or reuse the part from the 
					// previous iteration
					if (initial)
						this.file.read(part, 0, segmentSize);
					else
						part = partBefore;
					this.file.seek(pos+byte2write+segment);
					// and write the bytes
					this.file.write(part);
					initial = false;
				}
				else
				{
					byte[] part = new byte[segmentSize];
					// calculate the position to set the cursor to
					pos -= segmentSize;
					// and set it to the position
					this.file.seek(pos);
					// read the part
					if (partBefore == null)
					this.file.read(part, 0, segmentSize);
					// set the cursor to the new position
					this.file.seek(pos+byte2write);
					// and write the bytes
					this.file.write(part);
				}
			}
			while (remainingBytes > segmentSize);
			if (remainingBytes > 0)
			{
				if (partBefore == null)
				{
					byte[] rest = new byte[(int)remainingBytes];
					// calculate the position of the cursor to
					pos -= remainingBytes;
					// and set it to the position
					this.file.seek(pos);
					// read the rest
					this.file.read(rest, 0, (int)remainingBytes);
					// set the cursor to the new position
					this.file.seek(pos+byte2write);
					// and write the bytes
					this.file.write(rest);
				}
				else
				{
					this.file.seek(pos+byte2write);
					this.file.write(partBefore);
				}
			}
			
			if (byte2write < 0)
				this.file.setLength(this.file.length()+byte2write);
			
			// set the cursor back to the origin position
			this.file.seek(tmp);
		}
	}
		
	/**
	 * <p>Updates the last entry in the data storage</p>
	 * 
	 * @param data The object containing the data to write
	 * @param byte2write The number of bytes to write
	 * @return The number of bytes written
	 * @throws IOException
	 */
	private long updateLastEntry(InMemoryData<V,?> data, boolean append) throws IOException
	{
		// the key to update was written before so set the cursor
		// back to the start of the last entry
		this.file.seek(this.file.getFilePointer()-this.entrySize);
		
		long byte2write = 0;
		if (append)
		{
			this.lastElement.appendValue(data.getValue());
			byte2write = this.lastElement.getByteLengthKV();
			if (byte2write != this.entrySize)
				this.file.setLength(this.file.length()+(byte2write-this.entrySize));
			
			this.writeDataEntry(this.lastElement, true);
		}
		else
		{
			// enlarge the file if the new entry is larger than the old 
			// one, or shrink the file size if it is less
			byte2write = data.getByteLengthKV();
			if (byte2write != this.entrySize)
				this.file.setLength(this.file.length()+(byte2write-this.entrySize));
			
			this.writeDataEntry(data, true);
		}
		
		return byte2write;
	}
	
	/**
	 * <p>Inserts a new entry at the end of the data storage.</p>
	 * 
	 * @param data The object containing the data to write
	 * @param byte2write The number of bytes to write
	 * @return The number of bytes written
	 * @throws IOException
	 */
	private long addNewEntryAtTheEnd(InMemoryData<V,?> data) throws IOException
	{
		this.file.setLength(this.file.length()+data.getByteLengthKV());
		return this.writeDataEntry(data, true);
	}

	/**
	 * <p>Feeds a provided {@link List} with keys currently contained in 
	 * the cache file.</p>
	 * 
	 * @param keys A {@link List} which will contain the keys contained in
	 *             the backing file after this call
	 * @throws IOException
	 */
	public void printCacheContent(List<Long> keys, List<V> values)
	{
		if (logger.isInfoEnabled())
			logger.info("["+this.drum+"] - Data contained in backing cache:");
		
		// save current position
		try
		{
			long currentPosition = this.file.getFilePointer();
			this.file.seek(0);
			
			InMemoryData<V,?> data = null;
			do
			{
				data = this.getNextEntry();
				if (data != null)
				{
					if (keys != null)
						keys.add(data.getKey());
					V value = data.getValue();
					if (value != null)
					{
						if (values != null)
							values.add(value);
						if (logger.isInfoEnabled())
							logger.info("["+this.drum+"] - Key: "+data.getKey()+", Value: "+value);
					}
					else
					{
						if (values != null)
							values.add(null);
						if (logger.isInfoEnabled())
							logger.info("["+this.drum+"] - Key: "+data.getKey()+", Value: "+null);
					}
				}
			}
			while (data != null);
			
			// set the cursor back to its old position
			this.file.seek(currentPosition);
		}
		catch (Exception e)
		{
			if (logger.isErrorEnabled())
				logger.error("["+this.drum+"] - Error while printing content of "+
					this.name+"!", e);
			e.printStackTrace();
			throw new DrumException("Error while printing content of "+
					this.name+"! Reason: "+e.getLocalizedMessage());
		}
	}
}
