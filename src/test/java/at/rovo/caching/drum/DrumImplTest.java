package at.rovo.caching.drum;

import at.rovo.caching.drum.event.DrumEvent;
import at.rovo.caching.drum.testUtils.BaseCacheTest;
import at.rovo.caching.drum.testUtils.CacheUtils;
import at.rovo.caching.drum.testUtils.ConsoleDispatcher;
import at.rovo.caching.drum.util.DrumUtils;
import at.rovo.common.IntegrationTest;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Tests the functionality of DRUM through adding a couple of URLs which get first stored in memory. If a certain
 * threshold is reached the buffered data are persisted to a disk file which is attached to a buffer. If one of the disk
 * files exceeds a further threshold, a merge with the backing data store, which is a simple cache file, is invoked
 * which collects the data from all bucket files sequentially and merges them with the backing data store.
 * <p>
 * If a data item with the same key is already in the data store a DUPLICATE response will be responded, else a UNIQUE
 * one.
 * <p>
 * If an update request is sent to DRUM, the data store is assigned to overwrite the existing value, in case it already
 * exist - else it is created. With <em>appendUpdate</em> the provided data will be appended to the already existing key
 * instead of replacing it
 *
 * @author Roman Vottner
 */
@Category(IntegrationTest.class)
public class DrumImplTest extends BaseCacheTest implements DrumListener
{
    private final static Logger LOG = LogManager.getLogger(MethodHandles.lookup().lookupClass());

    /**
     * Example-Output:
     * <p>
     * UniqueKeyUpdate:    -7398944400403122887 Data: null Aux: http://www.java.com
     * UniqueKeyUpdate:    -4053763844255691886 Data: null Aux: http://glinden.blogspot.co.at/2008/05/crawling-is-harder-than-it-looks.html
     * UniqueKeyUpdate:    -4722211168175665381 Data: http://codeproject.com Aux: http://www.codeproject.com
     * DuplicateKeyUpdate: -4722211168175665381 Data: null Aux: http://www.codeproject.com
     * UniqueKeyUpdate:    -8006353971568025323 Data: null Aux: http://www.boost.org
     * DuplicateKeyUpdate: -4722211168175665381 Data: null Aux: http://www.codeproject.com
     * UniqueKeyUpdate:     -427820934381562479 Data: null Aux: http://www.tuwien.ac.at
     * UniqueKeyUpdate:     -408508820557862601 Data: null Aux: http://www.univie.ac.at
     * UniqueKeyUpdate:    -1003537773438859569 Data: null Aux: http://www.codeproject.com/Articles/36221/DRUM-A-C-Implementation-for-the-URL-seen-Test-of-a
     * UniqueKeyUpdate:     8763289732749923908 Data: null Aux: http://www.oracle.com/technology/products/berkeley-db/index.html
     * <p>
     * Content of disk storage:
     * Key: -8006353971568025323; value: null
     * Key: -7398944400403122887; value: null
     * Key: -4722211168175665381; value: http://codeproject.com
     * Key: -4053763844255691886; value: null
     * Key: -1003537773438859569; value: null
     * Key:  -427820934381562479; value: null
     * Key: -408508820557862601; value: null
     * Key:  8763289732749923908; value: null
     */

    @Test
    public void URLseenDrumTest()
    {
        try
        {
            Thread.sleep(5000);
        }
        catch (InterruptedException e1)
        {
            e1.printStackTrace();
        }
        Drum<String, String> drum = null;
        List<Long> URLhashes = new ArrayList<>();
        try
        {
            LOG.info("Example of Drum usage:");
            LOG.info("----------------------");

            LOG.info("Initializing Drum ... ");
            Dispatcher<String, String> dispatcher = new ConsoleDispatcher<>();
            //					new LogFileDispatcher<>();
            try
            {
                drum = new DrumBuilder<>("urlSeenTest", String.class, String.class).numBucket(4)
                        .bufferSize(64).dispatcher(dispatcher).listener(this).build();
            }
            catch (Exception e)
            {
                Assert.fail("Could not create DRUM instance. Caught error: " + e.getLocalizedMessage());
                LOG.error("Could not create DRUM instance. Caught error: " + e.getLocalizedMessage(), e);
                return;
            }
            LOG.info("done!");

            String url1 = "http://www.codeproject.com"; // produces 12 bytes in kvBucket and 26 bytes in auxBucket
            String url2 =
                    "http://www.oracle.com/technology/products/berkeley-db/index.html"; // produces 12 bytes in kvBucket and 64 bytes in auxBucket
            String url3 = "http://www.boost.org"; // produces 12 bytes in kvBucket and 20 bytes in auxBucket
            String url4 = "http://www.codeproject.com"; // produces 12 bytes in kvBucket and 26 bytes in auxBucket
            String url5 = "http://www.java.com"; // produces 12 bytes in kvBucket and 19 bytes in auxBucket
            String url6 =
                    "http://glinden.blogspot.co.at/2008/05/crawling-is-harder-than-it-looks.html"; // produces 12 bytes in kvBucket and 75 bytes in auxBucket

            URLhashes.add(DrumUtils.hash(url1));
            URLhashes.add(DrumUtils.hash(url2));
            URLhashes.add(DrumUtils.hash(url3));
            // URL is a duplicate one!
            // URLhashes.add(DrumUtil.hash(url4));
            URLhashes.add(DrumUtils.hash(url5));
            URLhashes.add(DrumUtils.hash(url6));

            LOG.info("checkUpdating urls ... ");
            drum.checkUpdate(DrumUtils.hash(url1), null, url1);
            drum.checkUpdate(DrumUtils.hash(url2), null, url2);
            drum.checkUpdate(DrumUtils.hash(url3), null, url3);
            drum.checkUpdate(DrumUtils.hash(url4), null, url4);
            drum.checkUpdate(DrumUtils.hash(url5), null, url5);

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

            drum.checkUpdate(DrumUtils.hash(url6), null, url6);
            LOG.info("done!");

            String url7 = "http://www.tuwien.ac.at"; // produces 12 bytes in kvBucket and 30 bytes in auxBucket
            String url8 = "http://www.univie.ac.at"; // produces 12 bytes in kvBucket and 30 bytes in auxBucket
            String url9 = "http://www.codeproject.com/Articles/36221/DRUM-A-C-Implementation-for-the-URL-seen-Test-of-a"; // produces 12 bytes in kvBucket and 92 bytes in auxBucket

            URLhashes.add(DrumUtils.hash(url7));
            URLhashes.add(DrumUtils.hash(url8));
            URLhashes.add(DrumUtils.hash(url9));

            LOG.info("Adding new urls ... ");
            drum.checkUpdate(DrumUtils.hash(url7), null, url7);
            drum.checkUpdate(DrumUtils.hash(url8), null, url8);
            drum.checkUpdate(DrumUtils.hash(url9), null, url9);
            // check+update on an already stored URL
            drum.checkUpdate(DrumUtils.hash(url1), "http://codeproject.com", url1);
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
            catch (Exception e)
            {
                e.printStackTrace();
                LOG.catching(e);
            }
        }

        try
        {
            List<Long> keys = new ArrayList<>();
            CacheUtils.printCacheContent("urlSeenTest", keys);
            Assert.assertNotNull(keys);
            for (Long urlHash : URLhashes)
            {
                Assert.assertTrue(urlHash + " not found in datastore!", keys.contains(urlHash));
                keys.remove(urlHash);
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

    @Override
    public void update(DrumEvent<? extends DrumEvent<?>> event)
    {
        LOG.debug(event);
    }
}
