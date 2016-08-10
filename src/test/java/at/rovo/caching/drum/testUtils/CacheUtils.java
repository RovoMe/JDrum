package at.rovo.caching.drum.testUtils;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.invoke.MethodHandles;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CacheUtils
{
    private final static Logger LOG = LogManager.getLogger(MethodHandles.lookup().lookupClass());

    private CacheUtils() {

    }

    public static void printCacheContent(String dbName, List<Long> data) throws IOException
    {
        String dbDir = System.getProperty("user.dir") + "/cache/" + dbName;
        RandomAccessFile cacheFile = new RandomAccessFile(dbDir + "/cache.db", "r");

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
}
