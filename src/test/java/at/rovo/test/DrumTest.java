package at.rovo.test;

import static org.junit.Assert.assertNotNull;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import junit.framework.Assert;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import at.rovo.caching.drum.Drum;
import at.rovo.caching.drum.DrumException;
import at.rovo.caching.drum.DrumUtil;
import at.rovo.caching.drum.IDrum;
import at.rovo.caching.drum.IDrumListener;
import at.rovo.caching.drum.StringSerializer;
import at.rovo.caching.drum.event.DrumEvent;

public class DrumTest implements IDrumListener
{	
	// create a logger
	private final static Logger logger = LogManager.getLogger(DrumTest.class.getName());	
	
	private File cache = null;
	
	@Before
	public void init()
	{
		String appDirPath = System.getProperty("user.dir");
		File appDir = new File(appDirPath);
		if (appDir.isDirectory())
		{
			String[] items = appDir.list();
			for (String item : items)
			{
				if (item.endsWith("cache"))
				{
					this.cache = new File(item);
					if (this.cache.isDirectory() && "cache".equals(this.cache.getName()))
					{
						try
						{
							Files.walkFileTree(this.cache.toPath(), new CacheFileDeleter());
						}
						catch (IOException e)
						{
							e.printStackTrace();
						}
					}
				}
			}
		}
		if (this.cache == null)
			this.cache = new File (appDir.getAbsoluteFile()+"/cache");
		if (!this.cache.exists())
			this.cache.mkdir();
	}
	
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
		// if this line results in null than add the following entry into
		// your VM argument settings:
		// -Dlog4j.configurationFile="${project_loc:Drum}/src/test/resources/log/log4j2-test.xml"
//		System.out.println(System.getProperty("log4j.configurationFile"));
		
		IDrum<StringSerializer, StringSerializer> drum = null;
		List<Long> URLhashes = new ArrayList<>();
		try
		{
			if (logger.isInfoEnabled())
			{
				logger.info("Example of Drum usage:");
				logger.info("----------------------");
				
				logger.info("Initializing Drum ... ");
			}
//			drum = new Drum<StringSerializer, StringSerializer>("urlSeenTest", 2, 64, new ConsoleDispatcher<StringSerializer, StringSerializer>(), StringSerializer.class, StringSerializer.class, this);
			drum = new Drum<StringSerializer, StringSerializer>("urlSeenTest", 2, 64, new LogFileDispatcher<StringSerializer, StringSerializer>(), StringSerializer.class, StringSerializer.class, this);
			if (logger.isInfoEnabled())
				logger.info("done!");		
			
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
			
			if (logger.isInfoEnabled())
				logger.info("checkUpdating urls ... ");
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
			if (logger.isInfoEnabled())
				logger.info("done!");
			
			// buffer size is selected to be small enough to trigger automatic
			// flipping, feeding and merging data - to test the synchronization
			// comment out the code below
//			logger.info("Synchronizing DB ... ");
//			// Synchronize
//			drum.synchronize();
//			logger.info("done!");
						
			String url7 = "http://www.tuwien.ac.at"; // produces 12 bytes in kvBucket and 30 bytes in auxBucket
			String url8 = "http://www.univie.ac.at"; // produces 12 bytes in kvBucket and 30 bytes in auxBucket
			String url9 = "http://www.codeproject.com/Articles/36221/DRUM-A-C-Implementation-for-the-URL-seen-Test-of-a"; // produces 12 bytes in kvBucket and 92 bytes in auxBucket
			
			URLhashes.add(DrumUtil.hash(url7));
			URLhashes.add(DrumUtil.hash(url8));
			URLhashes.add(DrumUtil.hash(url9));
			
			if (logger.isInfoEnabled())
				logger.info("Adding new urls ... ");
			drum.checkUpdate(DrumUtil.hash(url7), null, new StringSerializer(url7));
			drum.checkUpdate(DrumUtil.hash(url8), null, new StringSerializer(url8));
			drum.checkUpdate(DrumUtil.hash(url9), null, new StringSerializer(url9));
			// check+update on an already stored URL
			drum.checkUpdate(DrumUtil.hash(url1), new StringSerializer("http://codeproject.com"), new StringSerializer(url1));
			if (logger.isInfoEnabled())
				logger.info("done!");
		}
		catch(DrumException e)
		{
			System.err.println(e.getLocalizedMessage());
			e.printStackTrace();
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
			}
		}
		
		try
		{
			List<Long> keys = new ArrayList<>();
			this.printCacheContent("urlSeenTest", keys);
			assertNotNull(keys);
			for (Long URLhash : URLhashes)
			{
				Assert.assertTrue(keys.contains(URLhash));
				keys.remove(URLhash);
			}
			Assert.assertTrue(keys.isEmpty());
		}
		catch (IOException e)
		{
			if (logger.isErrorEnabled())
				logger.error(e.getLocalizedMessage());
			e.printStackTrace();
		}
	}
	
	public void printCacheContent(String dbName, List<Long> data) throws IOException
	{
		String dbDir = System.getProperty("user.dir")+"/cache/"+dbName;
		RandomAccessFile cacheFile = new RandomAccessFile(dbDir+"/cache.db", "r");
		
		cacheFile.seek(0);
		long fileSize = cacheFile.length();
		if (logger.isInfoEnabled())
			logger.info("Content of disk storage:");
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
			
			if (logger.isInfoEnabled())
				logger.info("Key: "+key+"; value: "+value);
			data.add(key);
		}
		
		cacheFile.close();
	}
		
	@After
	public void clean()
	{		
		File cache = this.cache;
		if (cache.isDirectory() && "cache".equals(cache.getName()))
		{
			try
			{
				Files.walkFileTree(cache.toPath(), new CacheFileDeleter());
			}
			catch (IOException e)
			{
				e.printStackTrace();
			}
		}
	}

	@Override
	public void update(DrumEvent<? extends DrumEvent<?>> event)
	{
		if (logger.isDebugEnabled())
			logger.debug(event);		
	}
}
