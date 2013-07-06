package at.rovo.test;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.Assert;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import at.rovo.caching.drum.DrumException;
import at.rovo.caching.drum.NotAppendableException;
import at.rovo.caching.drum.data.StringSerializer;
import at.rovo.caching.drum.internal.InMemoryData;
import at.rovo.caching.drum.internal.backend.cacheFile.CacheFile;
import at.rovo.caching.drum.util.DrumUtil;
import at.rovo.caching.drum.util.KeyComparator;

/**
 * <p>
 * Tests the cache file which acts as a data store.
 * </p>
 * <p>
 * The test creates a couple of entries and sends them to the cache file, which
 * is then checked if it stored the data appropriately. In case of already known
 * data, the data should get replaced on using
 * {@link CacheFile#writeEntry(InMemoryData, boolean)} with the boolean flag set
 * to false as this indicates an update of the already known data. Setting the
 * flag to true should append the data to the end.
 * </p>
 * 
 * @author Roman Vottner
 */
public class CacheFileTest
{
	/** The logger for this class **/
	private static Logger logger;
	private File testDir = null;
	
	@BeforeClass
	public static void initLogger() throws URISyntaxException
	{
		String path = CacheFileTest.class.getResource("/log/log4j2-test.xml").toURI().getPath();
		System.setProperty("log4j.configurationFile", path);
		logger = LogManager.getLogger(CacheFileTest.class);
	}
	
	@AfterClass
	public static void cleanLogger()
	{
		System.clearProperty("log4j.configurationFile");
	}
	
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
			
			logger.debug("Changing data item with key: {} from '{}' to 'test3'", data7.getKey(), data7.getValue());
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
			
			logger.debug("Changing data item with key: {} from '{}' to 'null'", data7.getKey(), data7.getValue());
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
			
			logger.debug("Changing data item with key: {} from '{}' to 'Noch ein Test'", data7.getKey(), data7.getValue());
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
			
			logger.debug("Changing data item with key: {} from '{}' to 'test'", data8.getKey(), data8.getValue());
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
			
			logger.debug("Changing data item with key: {} from '{}' to 'null'", data8.getKey(), data8.getKey());
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
			
			logger.debug("Changing data item with key: {} from '{}' to 'test'", data8.getKey(), data8.getValue());
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
		catch (IOException | InstantiationException | IllegalAccessException | DrumException | NotAppendableException e)
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
		logger.debug("Writing data: {}; auxiliary data: {}", data.getKey(), data.getAuxiliary());
		return data;
	}
	
	private static void printCacheContent(List<Long> keys, List<StringSerializer> values)
	{
		logger.debug("Data contained in cache file:");
		int size = keys.size();
		for (int i=0; i<size; i++)
		{
			logger.debug("Key: {}, Value: {}", keys.get(i), values.get(i));
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
