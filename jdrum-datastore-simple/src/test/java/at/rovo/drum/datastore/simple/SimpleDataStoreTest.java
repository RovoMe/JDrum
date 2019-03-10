package at.rovo.drum.datastore.simple;

import at.rovo.common.Pair;
import at.rovo.drum.DrumOperation;
import at.rovo.drum.DrumStoreEntry;
import at.rovo.drum.InMemoryEntry;
import at.rovo.drum.datastore.simple.utils.CacheFileDeleter;
import at.rovo.drum.datastore.simple.utils.DataStoreUtils;
import at.rovo.drum.datastore.simple.utils.PLDTestData;
import at.rovo.drum.util.DrumUtils;
import at.rovo.drum.util.KeyComparator;

import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static at.rovo.drum.datastore.simple.utils.DataStoreMatcher.containsEntries;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Tests the functionality of custom data store.
 * <p>
 * The test creates a couple of entries and writes them directly into the data store, which is then checked if it stored
 * the data appropriately. In case of already known data, the data should get replaced on using {@link
 * SimpleDataStore#writeEntry(DrumStoreEntry, boolean)} with the boolean flag set to false as this indicates an update
 * of the already known data. Setting the flag to true should append the data to the end.
 *
 * @author Roman Vottner
 */
public class SimpleDataStoreTest {

    /**
     * The logger for this class
     */
    private static Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private File testDir = null;

    @BeforeEach
    void init() {
        // get application directory
        String appDir = System.getProperty("user.dir");

        // check if the directories, the cache is located, exists
        File cacheDir = new File(appDir + "/cache");
        if (!cacheDir.exists()) {
            boolean success = cacheDir.mkdir();
            if (!success) {
                LOG.warn("Could not create cache directory");
            }
        }
        this.testDir = new File(cacheDir + "/test");
        if (!this.testDir.exists()) {
            boolean success = this.testDir.mkdir();
            if (!success) {
                LOG.warn("Could not create DRUM test directory");
            }
        }
    }

    @Test
    void testWriteAndUpdateEntries() throws Exception {
        try (SimpleDataStore<String> dataStore = new SimpleDataStoreImpl<>(this.testDir + "/cache.db", "test", String.class)) {
            // create a couple of test data and write them to the disk file
            // besides is the calculated hash key for the given URL and the position in the list after sorting
            List<DrumStoreEntry<String, String>> dataList = new ArrayList<>();
            dataList.add(createNewData("http://www.tuwien.ac.at"));          // key:  -427820934381562479  pos: 1
            dataList.add(createNewData("http://www.univie.ac.at"));          // key:  -408508820557862601  pos: 3
            dataList.add(createNewData("http://www.winf.at"));               // key:   356380646382129811  pos: 4
            dataList.add(createNewData("http://java.sun.com"));              // key: -7747270999347618272  pos: 0
            dataList.add(createNewData("http://www.tuwien.ac.at", "Test"));  // key:  -427820934381562479  pos: 2

            // sort the list
            dataList.sort(new KeyComparator<>());
            // and write the entries
            for (DrumStoreEntry<String, String> data : dataList) {
                dataStore.writeEntry(data, false);
            }

            // print the current content of the cache
            LinkedHashMap<Long, String> dataStoreContent =
                    (LinkedHashMap<Long, String>) DataStoreUtils.getContentAsMap("test", String.class);
            DataStoreUtils.printCacheContent(dataStoreContent);

            List<Pair<Long, String>> expected = new ArrayList<>();
            expected.add(new Pair<>(-7747270999347618272L, null));    // ... 8+4 bytes
            expected.add(new Pair<>(-427820934381562479L, "Test"));  // ... 8+4+4 bytes
            expected.add(new Pair<>(-408508820557862601L, null));    // ... 8+4 bytes
            expected.add(new Pair<>(356380646382129811L, null));    // ... 8+4 bytes

            assertThat(dataStore.length(), is(equalTo(52L)));
            assertThat(dataStoreContent, containsEntries(expected));

            // set the cursor back to the start - mock a new iteration of a merging process
            dataStore.reset();

            // modify a data object and write its content again
            LOG.debug("Adding new data and modify existing ones: ");
            dataList.clear();

            dataList.add(createNewData("http://www.krone.at"));                       // key: -7398944357989757338  pos: 2
            dataList.add(createNewData("http://www.univie.ac.at", "Noch ein Test"));  // key:  -408508820557862601  pos: 3
            dataList.add(createNewData("http://www.winf.at", "test2"));               // key:   356380646382129811  pos: 4
            dataList.add(createNewData("http://www.oracle.com"));                     // key: -8388954286180259435  pos: 1
            dataList.add(createNewData("http://www.google.com"));                     // key: -8389167973104044848  pos: 0

            // sort the list
            dataList.sort(new KeyComparator<>());
            // and write the entries
            for (DrumStoreEntry<String, ?> data : dataList) {
                dataStore.writeEntry(data, false);
            }

            // print the current content of the data store
            dataStoreContent = (LinkedHashMap<Long, String>) DataStoreUtils.getContentAsMap("test", String.class);
            DataStoreUtils.printCacheContent(dataStoreContent);

            expected = new ArrayList<>();
            expected.add(new Pair<>(-8389167973104044848L, null));             // ... 8+4 bytes
            expected.add(new Pair<>(-8388954286180259435L, null));             // ... 8+4 bytes
            expected.add(new Pair<>(-7747270999347618272L, null));             // ... 8+4 bytes
            expected.add(new Pair<>(-7398944357989757338L, null));             // ... 8+4 bytes
            expected.add(new Pair<>(-427820934381562479L, "Test"));           // ... 8+4+4 bytes
            expected.add(new Pair<>(-408508820557862601L, "Noch ein Test"));  // ... 8+4+13 bytes
            expected.add(new Pair<>(356380646382129811L, "test2"));          // ... 8+4+5 bytes

            assertThat(dataStore.length(), is(equalTo(106L)));
            assertThat(dataStoreContent, containsEntries(expected));

            dataStore.reset();

            LOG.debug("Changing data item with key: {} from '{}' to 'test3'",
                    dataList.get(3).getKey(), dataList.get(3).getValue());
            dataList.get(3).setValue("test3");
            dataStore.writeEntry(dataList.get(3), false);

            // print the current content of the data store
            dataStoreContent = (LinkedHashMap<Long, String>) DataStoreUtils.getContentAsMap("test", String.class);
            DataStoreUtils.printCacheContent(dataStoreContent);

            expected = new ArrayList<>();
            expected.add(new Pair<>(-8389167973104044848L, null));     // ... 8+4 bytes
            expected.add(new Pair<>(-8388954286180259435L, null));     // ... 8+4 bytes
            expected.add(new Pair<>(-7747270999347618272L, null));     // ... 8+4 bytes
            expected.add(new Pair<>(-7398944357989757338L, null));     // ... 8+4 bytes
            expected.add(new Pair<>(-427820934381562479L, "Test"));   // ... 8+4+4 bytes
            expected.add(new Pair<>(-408508820557862601L, "test3"));  // ... 8+4+5 bytes
            expected.add(new Pair<>(356380646382129811L, "test2"));  // ... 8+4+5 bytes

            assertThat(dataStore.length(), is(equalTo(98L)));
            assertThat(dataStoreContent, containsEntries(expected));

            dataStore.reset();

            LOG.debug("Changing data item with key: {} from '{}' to 'null'",
                    dataList.get(3).getKey(), dataList.get(3).getValue());
            dataList.get(3).setValue(null);
            dataStore.writeEntry(dataList.get(3), false);

            // print the current content of the data store
            dataStoreContent = (LinkedHashMap<Long, String>) DataStoreUtils.getContentAsMap("test", String.class);
            DataStoreUtils.printCacheContent(dataStoreContent);

            expected = new ArrayList<>();
            expected.add(new Pair<>(-8389167973104044848L, null));     // ... 8+4 bytes
            expected.add(new Pair<>(-8388954286180259435L, null));     // ... 8+4 bytes
            expected.add(new Pair<>(-7747270999347618272L, null));     // ... 8+4 bytes
            expected.add(new Pair<>(-7398944357989757338L, null));     // ... 8+4 bytes
            expected.add(new Pair<>(-427820934381562479L, "Test"));   // ... 8+4+4 bytes
            expected.add(new Pair<>(-408508820557862601L, null));     // ... 8+4 bytes
            expected.add(new Pair<>(356380646382129811L, "test2"));  // ... 8+4+5 bytes

            assertThat(dataStore.length(), is(equalTo(93L)));
            assertThat(dataStoreContent, containsEntries(expected));

            dataStore.reset();

            LOG.debug("Changing data item with key: {} from '{}' to 'Noch ein Test'",
                    dataList.get(3).getKey(), dataList.get(3).getValue());
            dataList.get(3).setValue("Noch ein Test");
            dataStore.writeEntry(dataList.get(3));

            // print the current content of the data store
            dataStoreContent = (LinkedHashMap<Long, String>) DataStoreUtils.getContentAsMap("test", String.class);
            DataStoreUtils.printCacheContent(dataStoreContent);

            expected = new ArrayList<>();
            expected.add(new Pair<>(-8389167973104044848L, null));             // ... 8+4 bytes
            expected.add(new Pair<>(-8388954286180259435L, null));             // ... 8+4 bytes
            expected.add(new Pair<>(-7747270999347618272L, null));             // ... 8+4 bytes
            expected.add(new Pair<>(-7398944357989757338L, null));             // ... 8+4 bytes
            expected.add(new Pair<>(-427820934381562479L, "Test"));           // ... 8+4+4 bytes
            expected.add(new Pair<>(-408508820557862601L, "Noch ein Test"));  // ... 8+4+13 bytes
            expected.add(new Pair<>(356380646382129811L, "test2"));          // ... 8+4+5 bytes

            assertThat(dataStore.length(), is(equalTo(106L)));
            assertThat(dataStoreContent, containsEntries(expected));

            dataStore.reset();

            LOG.debug("Changing data item with key: {} from '{}' to 'test'",
                    dataList.get(4).getKey(), dataList.get(4).getValue());
            dataList.get(4).setValue("test");
            dataStore.writeEntry(dataList.get(4));

            // print the current content of the data store
            dataStoreContent = (LinkedHashMap<Long, String>) DataStoreUtils.getContentAsMap("test", String.class);
            DataStoreUtils.printCacheContent(dataStoreContent);

            expected = new ArrayList<>();
            expected.add(new Pair<>(-8389167973104044848L, null));             // ... 8+4 bytes
            expected.add(new Pair<>(-8388954286180259435L, null));             // ... 8+4 bytes
            expected.add(new Pair<>(-7747270999347618272L, null));             // ... 8+4 bytes
            expected.add(new Pair<>(-7398944357989757338L, null));             // ... 8+4 bytes
            expected.add(new Pair<>(-427820934381562479L, "Test"));           // ... 8+4+4 bytes
            expected.add(new Pair<>(-408508820557862601L, "Noch ein Test"));  // ... 8+4+13 bytes
            expected.add(new Pair<>(356380646382129811L, "test"));           // ... 8+4+4 bytes

            assertThat(dataStore.length(), is(equalTo(105L)));
            assertThat(dataStoreContent, containsEntries(expected));

            dataStore.reset();

            LOG.debug("Changing data item with key: {} from '{}' to 'null'",
                    dataList.get(4).getKey(), dataList.get(4).getKey());
            dataList.get(4).setValue(null);
            dataStore.writeEntry(dataList.get(4));

            // print the current content of the data store
            dataStoreContent = (LinkedHashMap<Long, String>) DataStoreUtils.getContentAsMap("test", String.class);
            DataStoreUtils.printCacheContent(dataStoreContent);

            expected = new ArrayList<>();
            expected.add(new Pair<>(-8389167973104044848L, null));             // ... 8+4 bytes
            expected.add(new Pair<>(-8388954286180259435L, null));             // ... 8+4 bytes
            expected.add(new Pair<>(-7747270999347618272L, null));             // ... 8+4 bytes
            expected.add(new Pair<>(-7398944357989757338L, null));             // ... 8+4 bytes
            expected.add(new Pair<>(-427820934381562479L, "Test"));           // ... 8+4+4 bytes
            expected.add(new Pair<>(-408508820557862601L, "Noch ein Test"));  // ... 8+4+13 bytes
            expected.add(new Pair<>(356380646382129811L, null));             // ... 8+4 bytes

            assertThat(dataStore.length(), is(equalTo(101L)));
            assertThat(dataStoreContent, containsEntries(expected));

            dataStore.reset();

            LOG.debug("Changing data item with key: {} from '{}' to 'test'",
                    dataList.get(4).getKey(), dataList.get(4).getValue());
            dataList.get(4).setValue("test");
            dataStore.writeEntry(dataList.get(4));

            // print the current content of the data store
            dataStoreContent = (LinkedHashMap<Long, String>) DataStoreUtils.getContentAsMap("test", String.class);
            DataStoreUtils.printCacheContent(dataStoreContent);

            expected = new ArrayList<>();
            expected.add(new Pair<>(-8389167973104044848L, null));             // ... 8+4 bytes
            expected.add(new Pair<>(-8388954286180259435L, null));             // ... 8+4 bytes
            expected.add(new Pair<>(-7747270999347618272L, null));             // ... 8+4 bytes
            expected.add(new Pair<>(-7398944357989757338L, null));             // ... 8+4 bytes
            expected.add(new Pair<>(-427820934381562479L,  "Test"));           // ... 8+4+4 bytes
            expected.add(new Pair<>(-408508820557862601L,  "Noch ein Test"));  // ... 8+4+13 bytes
            expected.add(new Pair<>(356380646382129811L,   "test"));           // ... 8+4+4 bytes

            assertThat(dataStore.length(), is(equalTo(105L)));
            assertThat(dataStoreContent, containsEntries(expected));
        }
    }

    @Test
    void testAppendUpdate() throws Exception {
        try (SimpleDataStore<PLDTestData> dataStore = new SimpleDataStoreImpl<>(this.testDir + "/cache.db", "test", PLDTestData.class)) {
            // (1; 2; <3, 7>) has to be read as:
            // key = 1;
            // length of in-degree neighbors = 2;
            // in-degree neighbor set = {3, 7}
            //
            // old file on disk to merge with: (1; 2; <3, 7>), (5; 2; <2, 19>), (76; 4; <5, 13, 22, 88)
            // parameters: (nodeId; number of linked elements; <each linked element>)
            LOG.debug("Creating original data - (1; 2; <3, 7>), (5; 2; <2, 19>), (76; 4; <5, 13, 22, 88)");

            // key: 8 + op: 4 + value: (hash: 8 + neighbor-length: 4 + 2 * neighborHash: 8) + budges: 4) bytes = 44 ... 164 = 120 bytes diff :/
            DrumStoreEntry<PLDTestData, ?> mem1 = createNewData(1L, DrumOperation.UPDATE, 7L, 3L);
            DrumStoreEntry<PLDTestData, ?> mem2 = createNewData(5L, DrumOperation.UPDATE, 2L, 19L);
            DrumStoreEntry<PLDTestData, ?> mem3 = createNewData(76L, DrumOperation.UPDATE, 5L, 13L, 22L, 88L);

            // write the entries
            dataStore.writeEntry(mem1, false);
            dataStore.writeEntry(mem2, false);
            dataStore.writeEntry(mem3, false);

            assertThat(dataStore.length(), is(equalTo(148L)));

            Map<Long, PLDTestData> dataStoreContent = DataStoreUtils.getContentAsMap("test", PLDTestData.class);
            DataStoreUtils.printCacheContent(dataStoreContent);

            List<Pair<Long, PLDTestData>> expected = new ArrayList<>();
            expected.add(new Pair<>(1L, mem1.getValue()));
            expected.add(new Pair<>(5L, mem2.getValue()));
            expected.add(new Pair<>(76L, mem3.getValue()));

            assertThat(dataStoreContent, containsEntries(expected));

            dataStore.reset();

            LOG.debug("Adding new data to integrate into an existing entry - new batch: (5; 4; <2, 3, 7, 88>), (76; 2; <4, 13>)");

            // new batch, sorted as described above: (5; 4; <2, 3, 7, 88>), (76; 2; <4, 13>)
            DrumStoreEntry<PLDTestData, ?> mem2v2 = createNewData(5L, DrumOperation.APPEND_UPDATE, 2L, 3L, 7L, 88L);
            DrumStoreEntry<PLDTestData, ?> mem3v2 = createNewData(76L, DrumOperation.APPEND_UPDATE, 4L, 13L);

            // write the appended entries
            dataStore.writeEntry(mem2v2, true);
            dataStore.writeEntry(mem3v2, true);

            // new file produced in one pass: (1; 2; <3, 7>), (5; 5; <2, 3,  7, 19, 88), (76; 5; <4, 5, 13, 22, 88>)

            dataStoreContent = DataStoreUtils.getContentAsMap("test", PLDTestData.class);
            DataStoreUtils.printCacheContent(dataStoreContent);

            expected = new ArrayList<>();
            expected.add(new Pair<>(1L, new PLDTestData(1L, 0, new TreeSet<>(Arrays.asList(3L, 7L)))));
            expected.add(new Pair<>(5L, new PLDTestData(5L, 0, new TreeSet<>(Arrays.asList(2L, 3L, 7L, 19L, 88L)))));
            expected.add(new Pair<>(76L, new PLDTestData(76L, 0, new TreeSet<>(Arrays.asList(4L, 5L, 13L, 22L, 88L)))));

            assertThat(dataStoreContent, containsEntries(expected));
        }
    }

    private static DrumStoreEntry<String, String> createNewData(String auxiliaryData, String... val) {
        InMemoryEntry<String, String> data = new InMemoryEntry<>();
        if (val != null && val.length > 0) {
            data.setValue(val[0]);
        }
        data.setAuxiliary(auxiliaryData);
        data.setKey(DrumUtils.hash(data.getAuxiliary()));
        LOG.debug("Writing data: {}; auxiliary data: {}", data.getKey(), data.getAuxiliary());
        return data;
    }

    private static DrumStoreEntry<PLDTestData, String> createNewData(Long key, DrumOperation operation, Long... inDegreeNeighbors) {
        PLDTestData entry = new PLDTestData();
        entry.setHash(key);
        Set<Long> neighbor1 = new TreeSet<>(Arrays.asList(inDegreeNeighbors));
        entry.setIndegreeNeighbors(neighbor1);

        DrumStoreEntry<PLDTestData, String> data = new InMemoryEntry<>(key, entry, null, operation);
        LOG.debug("Writing data: {}; value: {}", data.getKey(), data.getValue());
        return data;
    }

    @AfterEach
    void clean() {
        File cache = this.testDir.getParentFile();
        if (cache.isDirectory() && "cache".equals(cache.getName())) {
            try {
                Files.walkFileTree(cache.toPath(), new CacheFileDeleter());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
