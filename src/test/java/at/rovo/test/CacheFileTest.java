package at.rovo.test;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import junit.framework.Assert;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import at.rovo.caching.drum.CacheFile;
import at.rovo.caching.drum.DrumUtil;
import at.rovo.caching.drum.InMemoryData;
import at.rovo.caching.drum.KeyComparator;
import at.rovo.caching.drum.StringSerializer;

public class CacheFileTest
{
	private final static Logger logger = LogManager.getLogger(CacheFileTest.class);
	private File testDir = null;
	
	@Before
	public void init()
	{
		// get application directory
		String appDir = System.getProperty("user.dir");
		
		// check if the directories, the cache is located, exists
		File cacheDir = new File(appDir+"/cache");
		if (!cacheDir.exists())
			cacheDir.mkdir();
		this.testDir = new File(cacheDir+"/test");
		if (!this.testDir.exists())
			this.testDir.mkdir();
	}
	
	@Test
	public void testURLseenCache()
	{
		CacheFile<StringSerializer> cacheFile = null;
		try
		{
			// create a new instance of our cache file
			cacheFile = new CacheFile<>(this.testDir+"/cache.db","test", StringSerializer.class);
			
			// create a couple of test data and write them to the disk file
			List<InMemoryData<StringSerializer, StringSerializer>> dataList = new ArrayList<>();
			InMemoryData<StringSerializer, StringSerializer> data1 = createNewData("http://www.tuwien.ac.at");
			dataList.add(data1);
			InMemoryData<StringSerializer, StringSerializer> data2 = createNewData("http://www.univie.ac.at");
			dataList.add(data2);
			InMemoryData<StringSerializer, StringSerializer> data3 = createNewData("http://www.winf.at");
			dataList.add(data3);
			InMemoryData<StringSerializer, StringSerializer> data4 = createNewData("http://java.sun.com");
			dataList.add(data4);
			InMemoryData<StringSerializer, StringSerializer> data5 = createNewData("http://www.tuwien.ac.at");
			data5.setValue(new StringSerializer("Test"));
			dataList.add(data5);
						
			// sort the list
			Collections.sort(dataList, new KeyComparator<InMemoryData<?,?>>());
			// and write the entries
			for (InMemoryData<StringSerializer, ?> data : dataList)
				cacheFile.writeEntry(data, false);
			
			// print the current content of the cache
			List<Long> keys = new ArrayList<>();
			List<StringSerializer> values = new ArrayList<>();
			cacheFile.printCacheContent(keys, values);
			printCacheContent(keys, values);
			
			// Data contained in backing cache:
			// Key: -7747270999347618272, Value: null          ... 8+4 bytes
			// Key:  -427820934381562479, Value: Test          ... 8+4+4 bytes
			// Key:  -408508820557862601, Value: null          ... 8+4 bytes
			// Key:   356380646382129811, Value: null          ... 8+4 bytes
			
			Assert.assertEquals(4, keys.size());
			Assert.assertEquals(4, values.size());
			
			Assert.assertEquals(-7747270999347618272L, keys.get(0).longValue());
			Assert.assertNull(values.get(0));
			Assert.assertEquals(-427820934381562479L, keys.get(1).longValue());
			Assert.assertEquals("Test", values.get(1).getData());
			Assert.assertEquals(-408508820557862601L, keys.get(2).longValue());
			Assert.assertNull(values.get(2));
			Assert.assertEquals(356380646382129811L, keys.get(3).longValue());
			Assert.assertNull(values.get(3));
			
			Assert.assertEquals(52L, cacheFile.length());
			
			// set the cursor back to the start - mock a new iteration of a
			// merging process
			cacheFile.reset();
			
			// modify a data object and write its content again
			if (logger.isDebugEnabled())
				logger.debug("Adding new data and modify existing ones: ");
			dataList.clear();
			
			InMemoryData<StringSerializer, StringSerializer> data6 = createNewData("http://www.krone.at");
			dataList.add(data6);
			InMemoryData<StringSerializer, StringSerializer> data7 = createNewData("http://www.univie.ac.at");
			data7.setValue(new StringSerializer("Noch ein Test"));
			dataList.add(data7);
			InMemoryData<StringSerializer, StringSerializer> data8 = createNewData("http://www.winf.at");
			data8.setValue(new StringSerializer("test2"));
			dataList.add(data8);
			InMemoryData<StringSerializer, StringSerializer> data9 = createNewData("http://www.oracle.com");
			dataList.add(data9);
			InMemoryData<StringSerializer, StringSerializer> data10 = createNewData("http://www.google.com");
			dataList.add(data10);
						
			// sort the list
			Collections.sort(dataList, new KeyComparator<InMemoryData<?,?>>());
			// and write the entries
			for (InMemoryData<StringSerializer, ?> data : dataList)
				cacheFile.writeEntry(data, false);
		
			// print the current content of the cache
			keys.clear();
			values.clear();
			cacheFile.printCacheContent(keys, values);
			printCacheContent(keys, values);
			
			// Data contained in backing cache:
			// Key: -8389167973104044848, Value: null          ... 8+4 bytes
			// Key: -8388954286180259435, Value: null          ... 8+4 bytes
			// Key: -7747270999347618272, Value: null          ... 8+4 bytes
			// Key: -7398944357989757338, Value: null          ... 8+4 bytes
			// Key:  -427820934381562479, Value: Test          ... 8+4+4 bytes
			// Key:  -408508820557862601, Value: Noch ein Test ... 8+4+13 bytes
			// Key:   356380646382129811, Value: test2         ... 8+4+5 bytes
			
			Assert.assertEquals(7, keys.size());
			Assert.assertEquals(7, values.size());
			
			Assert.assertEquals(-8389167973104044848L, keys.get(0).longValue());
			Assert.assertNull(values.get(0));
			Assert.assertEquals(-8388954286180259435L, keys.get(1).longValue());
			Assert.assertNull(values.get(1));
			Assert.assertEquals(-7747270999347618272L, keys.get(2).longValue());
			Assert.assertNull(values.get(2));
			Assert.assertEquals(-7398944357989757338L, keys.get(3).longValue());
			Assert.assertNull(values.get(3));
			Assert.assertEquals(-427820934381562479L, keys.get(4).longValue());
			Assert.assertEquals("Test", values.get(4).getData());
			Assert.assertEquals(-408508820557862601L, keys.get(5).longValue());
			Assert.assertEquals("Noch ein Test", values.get(5).getData());
			Assert.assertEquals(356380646382129811L, keys.get(6).longValue());
			Assert.assertEquals("test2", values.get(6).getData());
			
			Assert.assertEquals(106L, cacheFile.length());
			
			cacheFile.reset();
			
			if (logger.isDebugEnabled())
				logger.debug("Changing data item with key: "+data7.getKey()+" from '"+data7.getValue()+"' to 'test3'");
			data7.setValue(new StringSerializer("test3"));
			cacheFile.writeEntry(data7, false);
			
			keys.clear();
			values.clear();
			cacheFile.printCacheContent(keys, values);
			printCacheContent(keys, values);
			
			// Data contained in backing cache:
			// Key: -8389167973104044848, Value: null          ... 8+4 bytes
			// Key: -8388954286180259435, Value: null          ... 8+4 bytes
			// Key: -7747270999347618272, Value: null          ... 8+4 bytes
			// Key: -7398944357989757338, Value: null          ... 8+4 bytes
			// Key:  -427820934381562479, Value: Test          ... 8+4+4 bytes
			// Key:  -408508820557862601, Value: test3         ... 8+4+5 bytes
			// Key:   356380646382129811, Value: test2         ... 8+4+5 bytes
			
			Assert.assertEquals(7, keys.size());
			Assert.assertEquals(7, values.size());
			
			Assert.assertEquals(-8389167973104044848L, keys.get(0).longValue());
			Assert.assertNull(values.get(0));
			Assert.assertEquals(-8388954286180259435L, keys.get(1).longValue());
			Assert.assertNull(values.get(1));
			Assert.assertEquals(-7747270999347618272L, keys.get(2).longValue());
			Assert.assertNull(values.get(2));
			Assert.assertEquals(-7398944357989757338L, keys.get(3).longValue());
			Assert.assertNull(values.get(3));
			Assert.assertEquals(-427820934381562479L, keys.get(4).longValue());
			Assert.assertEquals("Test", values.get(4).getData());
			Assert.assertEquals(-408508820557862601L, keys.get(5).longValue());
			Assert.assertEquals("test3", values.get(5).getData());
			Assert.assertEquals(356380646382129811L, keys.get(6).longValue());
			Assert.assertEquals("test2", values.get(6).getData());
			
			Assert.assertEquals(98L, cacheFile.length());
			
			cacheFile.reset();
			
			if (logger.isDebugEnabled())
				logger.debug("Changing data item with key: "+data7.getKey()+" from '"+data7.getValue()+"' to 'null'");
			data7.setValue(null);
			cacheFile.writeEntry(data7, false);
			
			keys.clear();
			values.clear();
			cacheFile.printCacheContent(keys, values);
			printCacheContent(keys, values);
			
			// Data contained in backing cache:
			// Key: -8389167973104044848, Value: null          ... 8+4 bytes
			// Key: -8388954286180259435, Value: null          ... 8+4 bytes
			// Key: -7747270999347618272, Value: null          ... 8+4 bytes
			// Key: -7398944357989757338, Value: null          ... 8+4 bytes
			// Key:  -427820934381562479, Value: Test          ... 8+4+4 bytes
			// Key:  -408508820557862601, Value: null          ... 8+4 bytes
			// Key:   356380646382129811, Value: test2         ... 8+4+5 bytes
						
			Assert.assertEquals(7, keys.size());
			Assert.assertEquals(7, values.size());
			
			Assert.assertEquals(-8389167973104044848L, keys.get(0).longValue());
			Assert.assertNull(values.get(0));
			Assert.assertEquals(-8388954286180259435L, keys.get(1).longValue());
			Assert.assertNull(values.get(1));
			Assert.assertEquals(-7747270999347618272L, keys.get(2).longValue());
			Assert.assertNull(values.get(2));
			Assert.assertEquals(-7398944357989757338L, keys.get(3).longValue());
			Assert.assertNull(values.get(3));
			Assert.assertEquals(-427820934381562479L, keys.get(4).longValue());
			Assert.assertEquals("Test", values.get(4).getData());
			Assert.assertEquals(-408508820557862601L, keys.get(5).longValue());
			Assert.assertNull(values.get(5));
			Assert.assertEquals(356380646382129811L, keys.get(6).longValue());
			Assert.assertEquals("test2", values.get(6).getData());
			
			Assert.assertEquals(93L, cacheFile.length());
			
			cacheFile.reset();
			
			if (logger.isDebugEnabled())
				logger.debug("Changing data item with key: "+data7.getKey()+" from '"+data7.getValue()+"' to 'Noch ein Test'");
			data7.setValue(new StringSerializer("Noch ein Test"));
			cacheFile.writeEntry(data7);
			
			keys.clear();
			values.clear();
			cacheFile.printCacheContent(keys, values);
			printCacheContent(keys, values);
			
			// Data contained in backing cache:
			// Key: -8389167973104044848, Value: null          ... 8+4 bytes
			// Key: -8388954286180259435, Value: null          ... 8+4 bytes
			// Key: -7747270999347618272, Value: null          ... 8+4 bytes
			// Key: -7398944357989757338, Value: null          ... 8+4 bytes
			// Key:  -427820934381562479, Value: Test          ... 8+4+4 bytes
			// Key:  -408508820557862601, Value: Noch ein Test ... 8+4+13 bytes
			// Key:   356380646382129811, Value: test2         ... 8+4+5 bytes
						
			Assert.assertEquals(7, keys.size());
			Assert.assertEquals(7, values.size());
			
			Assert.assertEquals(-8389167973104044848L, keys.get(0).longValue());
			Assert.assertNull(values.get(0));
			Assert.assertEquals(-8388954286180259435L, keys.get(1).longValue());
			Assert.assertNull(values.get(1));
			Assert.assertEquals(-7747270999347618272L, keys.get(2).longValue());
			Assert.assertNull(values.get(2));
			Assert.assertEquals(-7398944357989757338L, keys.get(3).longValue());
			Assert.assertNull(values.get(3));
			Assert.assertEquals(-427820934381562479L, keys.get(4).longValue());
			Assert.assertEquals("Test", values.get(4).getData());
			Assert.assertEquals(-408508820557862601L, keys.get(5).longValue());
			Assert.assertEquals("Noch ein Test", values.get(5).getData());
			Assert.assertEquals(356380646382129811L, keys.get(6).longValue());
			Assert.assertEquals("test2", values.get(6).getData());
			
			Assert.assertEquals(106L, cacheFile.length());
			
			cacheFile.reset();
			
			if (logger.isDebugEnabled())
				logger.debug("Changing data item with key: "+data8.getKey()+" from '"+data8.getValue()+"' to 'test'");
			data8.setValue(new StringSerializer("test"));
			cacheFile.writeEntry(data8);
			
			keys.clear();
			values.clear();
			cacheFile.printCacheContent(keys, values);
			printCacheContent(keys, values);
			
			// Data contained in backing cache:
			// Key: -8389167973104044848, Value: null          ... 8+4 bytes
			// Key: -8388954286180259435, Value: null          ... 8+4 bytes
			// Key: -7747270999347618272, Value: null          ... 8+4 bytes
			// Key: -7398944357989757338, Value: null          ... 8+4 bytes
			// Key:  -427820934381562479, Value: Test          ... 8+4+4 bytes
			// Key:  -408508820557862601, Value: Noch ein Test ... 8+4+13 bytes
			// Key:   356380646382129811, Value: test          ... 8+4+4 bytes
						
			Assert.assertEquals(7, keys.size());
			Assert.assertEquals(7, values.size());
			
			Assert.assertEquals(-8389167973104044848L, keys.get(0).longValue());
			Assert.assertNull(values.get(0));
			Assert.assertEquals(-8388954286180259435L, keys.get(1).longValue());
			Assert.assertNull(values.get(1));
			Assert.assertEquals(-7747270999347618272L, keys.get(2).longValue());
			Assert.assertNull(values.get(2));
			Assert.assertEquals(-7398944357989757338L, keys.get(3).longValue());
			Assert.assertNull(values.get(3));
			Assert.assertEquals(-427820934381562479L, keys.get(4).longValue());
			Assert.assertEquals("Test", values.get(4).getData());
			Assert.assertEquals(-408508820557862601L, keys.get(5).longValue());
			Assert.assertEquals("Noch ein Test", values.get(5).getData());
			Assert.assertEquals(356380646382129811L, keys.get(6).longValue());
			Assert.assertEquals("test", values.get(6).getData());
			
			Assert.assertEquals(105L, cacheFile.length());
			
			cacheFile.reset();
			
			if (logger.isDebugEnabled())
				logger.debug("Changing data item with key: "+data8.getKey()+" from '"+data8.getValue()+"' to 'null'");
			data8.setValue(null);
			cacheFile.writeEntry(data8);
			
			keys.clear();
			values.clear();
			cacheFile.printCacheContent(keys, values);
			printCacheContent(keys, values);
			
			// Data contained in backing cache:
			// Key: -8389167973104044848, Value: null          ... 8+4 bytes
			// Key: -8388954286180259435, Value: null          ... 8+4 bytes
			// Key: -7747270999347618272, Value: null          ... 8+4 bytes
			// Key: -7398944357989757338, Value: null          ... 8+4 bytes
			// Key:  -427820934381562479, Value: Test          ... 8+4+4 bytes
			// Key:  -408508820557862601, Value: Noch ein Test ... 8+4+13 bytes
			// Key:   356380646382129811, Value: null          ... 8+4 bytes
			
			Assert.assertEquals(7, keys.size());
			Assert.assertEquals(7, values.size());
			
			Assert.assertEquals(-8389167973104044848L, keys.get(0).longValue());
			Assert.assertNull(values.get(0));
			Assert.assertEquals(-8388954286180259435L, keys.get(1).longValue());
			Assert.assertNull(values.get(1));
			Assert.assertEquals(-7747270999347618272L, keys.get(2).longValue());
			Assert.assertNull(values.get(2));
			Assert.assertEquals(-7398944357989757338L, keys.get(3).longValue());
			Assert.assertNull(values.get(3));
			Assert.assertEquals(-427820934381562479L, keys.get(4).longValue());
			Assert.assertEquals("Test", values.get(4).getData());
			Assert.assertEquals(-408508820557862601L, keys.get(5).longValue());
			Assert.assertEquals("Noch ein Test", values.get(5).getData());
			Assert.assertEquals(356380646382129811L, keys.get(6).longValue());
			Assert.assertNull(values.get(6));
			
			Assert.assertEquals(101L, cacheFile.length());
			
			cacheFile.reset();
			
			if (logger.isDebugEnabled())
				logger.debug("Changing data item with key: "+data8.getKey()+" from '"+data8.getValue()+"' to 'test'");
			data8.setValue(new StringSerializer("test"));
			cacheFile.writeEntry(data8);
			
			keys.clear();
			values.clear();
			cacheFile.printCacheContent(keys, values);
			printCacheContent(keys, values);
			
			
			// Data contained in backing cache:
			// Key: -8389167973104044848, Value: null          ... 8+4 bytes
			// Key: -8388954286180259435, Value: null          ... 8+4 bytes
			// Key: -7747270999347618272, Value: null          ... 8+4 bytes
			// Key: -7398944357989757338, Value: null          ... 8+4 bytes
			// Key:  -427820934381562479, Value: Test          ... 8+4+4 bytes
			// Key:  -408508820557862601, Value: Noch ein Test ... 8+4+13 bytes
			// Key:   356380646382129811, Value: test          ... 8+4+4 bytes
			
			Assert.assertEquals(7, keys.size());
			Assert.assertEquals(7, values.size());
			
			Assert.assertEquals(-8389167973104044848L, keys.get(0).longValue());
			Assert.assertNull(values.get(0));
			Assert.assertEquals(-8388954286180259435L, keys.get(1).longValue());
			Assert.assertNull(values.get(1));
			Assert.assertEquals(-7747270999347618272L, keys.get(2).longValue());
			Assert.assertNull(values.get(2));
			Assert.assertEquals(-7398944357989757338L, keys.get(3).longValue());
			Assert.assertNull(values.get(3));
			Assert.assertEquals(-427820934381562479L, keys.get(4).longValue());
			Assert.assertEquals("Test", values.get(4).getData());
			Assert.assertEquals(-408508820557862601L, keys.get(5).longValue());
			Assert.assertEquals("Noch ein Test", values.get(5).getData());
			Assert.assertEquals(356380646382129811L, keys.get(6).longValue());
			Assert.assertEquals("test", values.get(6).getData());
			
			Assert.assertEquals(105L, cacheFile.length());
		}
		catch (IOException | InstantiationException | IllegalAccessException e)
		{
			e.printStackTrace();
		}
		finally
		{
			cacheFile.close();
		}
	}
	
	private static InMemoryData<StringSerializer, StringSerializer> createNewData(String auxiliaryData)
	{
		InMemoryData<StringSerializer, StringSerializer> data = new InMemoryData<>();
		data.setAuxiliary(new StringSerializer(auxiliaryData));
		data.setKey(DrumUtil.hash(data.getAuxiliary().getData()));
		if (logger.isDebugEnabled())
			logger.debug("Writing data: "+data.getKey()+"; auxiliary data: "+data.getAuxiliary());
		return data;
	}
	
	private static void printCacheContent(List<Long> keys, List<StringSerializer> values)
	{
		if (logger.isDebugEnabled())
			logger.debug("Data contained in cache file:");
		int size = keys.size();
		for (int i=0; i<size; i++)
		{
			if (logger.isDebugEnabled())
				logger.debug("Key: "+keys.get(i)+", Value: "+values.get(i));
		}
	}
	
	@After
	public void clean()
	{
		File cache = this.testDir.getParentFile();
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
}
