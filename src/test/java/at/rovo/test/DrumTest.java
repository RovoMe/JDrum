package at.rovo.test;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import at.rovo.caching.drum.Drum;
import at.rovo.caching.drum.IDispatcher;
import at.rovo.caching.drum.IDrum;
import at.rovo.caching.drum.IDrumListener;
import at.rovo.caching.drum.data.StringSerializer;
import at.rovo.caching.drum.event.DrumEvent;
import at.rovo.caching.drum.util.DrumUtil;

/**
 * <p>
 * Tests the functionality of DRUM through adding a couple of URLs which get
 * first stored in memory. If a certain threshold is reached the buffered data
 * are persisted to a disk file which is attached to a buffer. If one of the
 * disk files exceeds a further threshold, a merge with the backing data store,
 * which is a simple cache file, is invoked which collects the data from all
 * bucket files sequentially and merges them with the backing data store.
 * </p>
 * <p>
 * If a data item with the same key is already in the data store a DUPLICATE
 * response will be responded, else a UNIQUE one.
 * </p>
 * <p>
 * If an update request is sent to DRUM, the data store is assigned to overwrite
 * the existing value, in case it already exist - else it is created. With
 * <em>appendUpdate</em> the provided data will be appended to the already
 * existing key instead of replacing it
 * </p>
 * 
 * @author Roman Vottner
 */
public class DrumTest extends BaseCacheTest  implements IDrumListener
{
		/**
		 * Example-Output:
		 *
		 * UniqueKeyUpdate: -7398944400403122887 Data: null Aux: http://www.java.com
		 * UniqueKeyUpdate: -4053763844255691886 Data: null Aux: http://glinden.blogspot.co.at/2008/05/crawling-is-harder-than-it-looks.html
		 * UniqueKeyUpdate: -4722211168175665381 Data: http://codeproject.com Aux: http://www.codeproject.com
		 * DuplicateKeyUpdate: -4722211168175665381 Data: null Aux: http://www.codeproject.com
		 * UniqueKeyUpdate: -8006353971568025323 Data: null Aux: http://www.boost.org
		 * DuplicateKeyUpdate: -4722211168175665381 Data: null Aux: http://www.codeproject.com
		 * UniqueKeyUpdate: -427820934381562479 Data: null Aux: http://www.tuwien.ac.at
		 * UniqueKeyUpdate: -408508820557862601 Data: null Aux: http://www.univie.ac.at
		 * UniqueKeyUpdate: -1003537773438859569 Data: null Aux: http://www.codeproject.com/Articles/36221/DRUM-A-C-Implementation-for-the-URL-seen-Test-of-a
		 * UniqueKeyUpdate: 8763289732749923908 Data: null Aux: http://www.oracle.com/technology/products/berkeley-db/index.html
		 *
		 * Content of disk storage:
		 * Key: -8006353971568025323; value: null
		 * Key: -7398944400403122887; value: null
		 * Key: -4722211168175665381; value: http://codeproject.com
		 * Key: -4053763844255691886; value: null
		 * Key: -1003537773438859569; value: null
		 * Key: -427820934381562479; value: null
		 * Key: -408508820557862601; value: null
		 * Key: 8763289732749923908; value: null
		 */
	
	@Test
	public void URLseenDrumTest()
	{		
		IDrum<StringSerializer, StringSerializer> drum = null;
		List<Long> URLhashes = new ArrayList<>();
		try
		{
			LOG.info("Example of Drum usage:");
			LOG.info("----------------------");

			LOG.info("Initializing Drum ... ");
			IDispatcher<StringSerializer, StringSerializer> dispatcher =
					new ConsoleDispatcher<>();
//					new LogFileDispatcher<>();
			try
			{
				drum = new Drum.Builder<>("urlSeenTest", 
						StringSerializer.class, StringSerializer.class)
					.numBucket(4)
					.bufferSize(64)
					.dispatcher(dispatcher)
					.listener(this)
					.build();
			}
			catch (Exception e)
			{
				Assert.fail("Could not create DRUM instance. Caught error: "
						+ e.getLocalizedMessage());
				LOG.error("Could not create DRUM instance. Caught error: "
						+ e.getLocalizedMessage(), e);
				return;
			}
			LOG.info("done!");
			
			String url1 = "http://www.codeproject.com"; // produces 12 bytes in kvBucket and 26 bytes in auxBucket
			String url2 = "http://www.oracle.com/technology/products/berkeley-db/index.html"; // produces 12 bytes in kvBucket and 64 bytes in auxBucket
			String url3 = "http://www.boost.org"; // produces 12 bytes in kvBucket and 20 bytes in auxBucket 
			String url4 = "http://www.codeproject.com"; // produces 12 bytes in kvBucket and 26 bytes in auxBucket
			String url5 = "http://www.java.com"; // produces 12 bytes in kvBucket and 19 bytes in auxBucket
			String url6 = "http://glinden.blogspot.co.at/2008/05/crawling-is-harder-than-it-looks.html"; // produces 12 bytes in kvBucket and 75 bytes in auxBucket
			
			URLhashes.add(DrumUtil.hash(url1));
			URLhashes.add(DrumUtil.hash(url2));
			URLhashes.add(DrumUtil.hash(url3));
			// URL is a duplicate one!
			// URLhashes.add(DrumUtil.hash(url4));
			URLhashes.add(DrumUtil.hash(url5));
			URLhashes.add(DrumUtil.hash(url6));

			LOG.info("checkUpdating urls ... ");
			drum.checkUpdate(DrumUtil.hash(url1), null, new StringSerializer(url1));
			drum.checkUpdate(DrumUtil.hash(url2), null, new StringSerializer(url2));
			drum.checkUpdate(DrumUtil.hash(url3), null, new StringSerializer(url3));
			drum.checkUpdate(DrumUtil.hash(url4), null, new StringSerializer(url4));
			drum.checkUpdate(DrumUtil.hash(url5), null, new StringSerializer(url5));
			
			// as new URLs are added to the DRUM instance very fast it may happen
			// that the main thread reaches the end of this block (or the synchronize 
			// method below) without giving its threads a chance to perform their
			// tasks - buffers are examined every 10 ms so the wait should be a bit
			// longer than those 10 ms
			try
			{
				Thread.sleep(50L);
			}
			catch (InterruptedException e)
			{
				e.printStackTrace();
			}

			drum.checkUpdate(DrumUtil.hash(url6), null, new StringSerializer(url6));
			LOG.info("done!");
									
			String url7 = "http://www.tuwien.ac.at"; // produces 12 bytes in kvBucket and 30 bytes in auxBucket
			String url8 = "http://www.univie.ac.at"; // produces 12 bytes in kvBucket and 30 bytes in auxBucket
			String url9 = "http://www.codeproject.com/Articles/36221/DRUM-A-C-Implementation-for-the-URL-seen-Test-of-a"; // produces 12 bytes in kvBucket and 92 bytes in auxBucket
			
			URLhashes.add(DrumUtil.hash(url7));
			URLhashes.add(DrumUtil.hash(url8));
			URLhashes.add(DrumUtil.hash(url9));

			LOG.info("Adding new urls ... ");
			drum.checkUpdate(DrumUtil.hash(url7), null, new StringSerializer(url7));
			drum.checkUpdate(DrumUtil.hash(url8), null, new StringSerializer(url8));
			drum.checkUpdate(DrumUtil.hash(url9), null, new StringSerializer(url9));
			// check+update on an already stored URL
			drum.checkUpdate(DrumUtil.hash(url1), new StringSerializer("http://codeproject.com"), new StringSerializer(url1));
			LOG.info("done!");
		}
		finally
		{
			// dispose synchronizes DRUM before closing all files
			try
			{
				if (drum != null)
				{
					drum.dispose();
				}
			}
			catch(Exception e)
			{
				e.printStackTrace();
				LOG.catching(e);
			}
		}
		
		try
		{
			List<Long> keys = new ArrayList<>();
			this.printCacheContent("urlSeenTest", keys);
			Assert.assertNotNull(keys);
			for (Long URLhash : URLhashes)
			{
				Assert.assertTrue(keys.contains(URLhash));
				keys.remove(URLhash);
			}
			Assert.assertTrue(keys.isEmpty());
		}
		catch (IOException e)
		{
			LOG.error("Noticed error: {}", e.getMessage(), e);
			LOG.catching(e);
			Assert.fail();
		}
	}
	
	public void printCacheContent(String dbName, List<Long> data) throws IOException
	{
		String dbDir = System.getProperty("user.dir")+"/cache/"+dbName;
		RandomAccessFile cacheFile = new RandomAccessFile(dbDir+"/cache.db", "r");
		
		cacheFile.seek(0);
		long fileSize = cacheFile.length();
		LOG.info("Content of disk storage:");
		for (long pos = 0; pos < fileSize; pos = cacheFile.getFilePointer())
		{
			Long key = cacheFile.readLong();
			Integer valueSize = cacheFile.readInt();
			String value = null;
			if (valueSize > 0)
			{
				byte[] valueBytes = new byte[valueSize];
				cacheFile.read(valueBytes, 0, valueSize);
				value = new String(valueBytes);
			}

			LOG.info("Key: {}; value: {}", key, value);
			data.add(key);
		}
		
		cacheFile.close();
	}

	@Override
	public void update(DrumEvent<? extends DrumEvent<?>> event)
	{
		LOG.debug(event);
	}
}
