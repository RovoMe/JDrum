package at.rovo.caching.drum.testUtils;

import at.rovo.caching.drum.internal.backend.berkeley.BTreeCompare;
import at.rovo.caching.drum.util.DrumUtils;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.ExceptionEvent;
import com.sleepycat.je.ExceptionListener;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Utility class to interact with the content of a Berkeley database.
 *
 * @author Roman Vottner
 */
public final class BerkeleyDBUtils
{
    /**
     * Helper class which encapsulates the database connection to a Berkeley database.
     */
    private static class BerkeleyDB implements ExceptionListener, AutoCloseable
    {
        private final Database db;

        private BerkeleyDB(String drumName, int cacheSize)
        {
            EnvironmentConfig config = new EnvironmentConfig();
            config.setAllowCreate(false);
            config.setExceptionListener(this);
            if (cacheSize > 0)
            {
                config.setCacheSize(cacheSize);
            }

            File dbDir = new File(System.getProperty("user.dir") + "/cache");
            if (!dbDir.exists())
            {
                throw new IllegalStateException("Could not find data base directory");
            }

            File dbFile = new File(dbDir, drumName);
            if (!dbFile.exists())
            {
                throw new IllegalStateException("Could not find data base file");
            }

            Environment env = new Environment(dbFile, config);
            DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setAllowCreate(false);
            dbConfig.setBtreeComparator(new BTreeCompare());
            dbConfig.setDeferredWrite(true);

            db = env.openDatabase(null, drumName + ".db", dbConfig);
        }

        @Override
        public void exceptionThrown(ExceptionEvent exceptionEvent)
        {
            throw new UnsupportedOperationException("Not yet implemented");
        }

        public void close()
        {
            db.close();
        }
    }

    /**
     * Returns a {@link Map} containing all the entries of the database.
     *
     * @param drumName
     *         The name of the database to connect to
     * @param valueClass
     *         The class of the value object
     * @param <V>
     *         The type of the value
     *
     * @return A map containing the keys and values of the database
     */
    public static <V extends Serializable> Map<Long, V> getEntries(String drumName, Class<V> valueClass)
    {
        Map<Long, V> entries = new LinkedHashMap<>();

        try (BerkeleyDB berkeleyDB = new BerkeleyDB(drumName, 0);
             Cursor cursor = berkeleyDB.db.openCursor(null, null))
        {
            DatabaseEntry foundKey = new DatabaseEntry();
            DatabaseEntry foundData = new DatabaseEntry();

            while (cursor.getNext(foundKey, foundData, LockMode.DEFAULT) == OperationStatus.SUCCESS)
            {
                Long key = DrumUtils.byte2long(foundKey.getData());
                V data = DrumUtils.deserialize(foundData.getData(), valueClass);
                entries.put(key, data);
                System.out.println("Key: " + key + " Data: " + data);
            }
        }
        catch (DatabaseException | IOException | ClassNotFoundException ex)
        {
            System.err.println("Error accessing database." + ex);
        }
        return entries;
    }
}
