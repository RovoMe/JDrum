package at.rovo.drum.impl;

import at.rovo.drum.Dispatcher;
import at.rovo.drum.Drum;
import at.rovo.drum.DrumListener;
import at.rovo.drum.datastore.simple.SimpleDataStoreMerger;
import at.rovo.drum.datastore.simple.utils.DataStoreUtils;
import at.rovo.drum.event.DrumEvent;
import at.rovo.drum.impl.utils.BaseDataStoreTest;
import at.rovo.drum.impl.utils.ConsoleDispatcher;
import at.rovo.drum.util.DrumUtils;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import static org.junit.jupiter.api.Assertions.assertTrue;

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
public class DrumImplTest extends BaseDataStoreTest implements DrumListener {

    private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

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
     * Key:  -408508820557862601; value: null
     * Key:  8763289732749923908; value: null
     */

    @Test
    void testDrumWithSimpleDataStore() throws Exception {
        String drumName = "urlSeenTest";
        Drum<String, String> drum = null;
        List<Long> urlHashes = new ArrayList<>();
        try {
            Dispatcher<String, String> dispatcher = new ConsoleDispatcher<>();
            drum = new DrumBuilder<>(drumName, String.class, String.class).numBucket(4).bufferSize(64)
                    .dispatcher(dispatcher).listener(this).datastore(SimpleDataStoreMerger.class).build();

            runTest(drum, urlHashes);
        } finally {
            if (drum != null) {
                drum.dispose();
            }
        }

        Map<Long, String> dbContent = DataStoreUtils.getContentAsMap(drumName, String.class);
        for (Long urlHash : urlHashes) {
            assertTrue(dbContent.containsKey(urlHash), urlHash + " not found in data store!");
            dbContent.remove(urlHash);
        }
        assertTrue(dbContent.isEmpty(), "DB content was not empty!");
    }

    private void runTest(Drum<String, String> drum, List<Long> urlHashes) throws Exception {
        String url1 = "http://www.codeproject.com"; // produces 12 bytes in kvBucket and 26 bytes in auxBucket
        String url2 = "http://www.oracle.com/technology/products/berkeley-db/index.html"; // produces 12 bytes in kvBucket and 64 bytes in auxBucket
        String url3 = "http://www.boost.org"; // produces 12 bytes in kvBucket and 20 bytes in auxBucket
        String url4 = "http://www.codeproject.com"; // produces 12 bytes in kvBucket and 26 bytes in auxBucket
        String url5 = "http://www.java.com"; // produces 12 bytes in kvBucket and 19 bytes in auxBucket
        String url6 = "http://glinden.blogspot.co.at/2008/05/crawling-is-harder-than-it-looks.html"; // produces 12 bytes in kvBucket and 75 bytes in auxBucket

        urlHashes.add(DrumUtils.hash(url1));
        urlHashes.add(DrumUtils.hash(url2));
        urlHashes.add(DrumUtils.hash(url3));
        // URL is a duplicate one!
        // urlHashes.add(DrumUtil.hash(url4));
        urlHashes.add(DrumUtils.hash(url5));
        urlHashes.add(DrumUtils.hash(url6));

        LOG.info("checkUpdating urls ... ");
        drum.checkUpdate(DrumUtils.hash(url1), null, url1);
        drum.checkUpdate(DrumUtils.hash(url2), null, url2);
        drum.checkUpdate(DrumUtils.hash(url3), null, url3);
        drum.checkUpdate(DrumUtils.hash(url4), null, url4);
        drum.checkUpdate(DrumUtils.hash(url5), null, url5);

        // as new URLs are added to the DRUM instance very fast it may happen that the main thread reaches the end
        // of this block without giving its threads a chance to perform their tasks
        Thread.sleep(20L);

        drum.checkUpdate(DrumUtils.hash(url6), null, url6);
        LOG.info("done!");

        String url7 = "http://www.tuwien.ac.at"; // produces 12 bytes in kvBucket and 30 bytes in auxBucket
        String url8 = "http://www.univie.ac.at"; // produces 12 bytes in kvBucket and 30 bytes in auxBucket
        String url9 = "http://www.codeproject.com/Articles/36221/DRUM-A-C-Implementation-for-the-URL-seen-Test-of-a"; // produces 12 bytes in kvBucket and 92 bytes in auxBucket

        urlHashes.add(DrumUtils.hash(url7));
        urlHashes.add(DrumUtils.hash(url8));
        urlHashes.add(DrumUtils.hash(url9));

        LOG.info("Adding new urls ... ");
        drum.checkUpdate(DrumUtils.hash(url7), null, url7);
        drum.checkUpdate(DrumUtils.hash(url8), null, url8);
        drum.checkUpdate(DrumUtils.hash(url9), null, url9);
        // check+update on an already stored URL
        drum.checkUpdate(DrumUtils.hash(url1), "http://codeproject.com", url1);
        LOG.info("done!");
    }

    @Override
    public void update(@Nonnull final DrumEvent<? extends DrumEvent<?>> event) {
        LOG.debug(event.toString());
    }
}
