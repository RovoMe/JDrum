package at.rovo.drum.utils;

import at.rovo.drum.datastore.simple.utils.CacheFileDeleter;
import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Files;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for all data store based unit tests. This abstract class does take responsibility of the creation and
 * deletion of the actual data store files and the initialization of the logger instance.
 *
 * @author Roman Vottner
 */
public abstract class BaseDataStoreTest
{
    /** The logger of this class **/
    private final static Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    protected File cacheDir = null;

    @Before
    public final void init() throws Exception
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
                    this.cacheDir = new File(item);
                    if (this.cacheDir.isDirectory() && "cache".equals(this.cacheDir.getName()))
                    {
                        try
                        {
                            Files.walkFileTree(this.cacheDir.toPath(), new CacheFileDeleter());
                        }
                        catch (IOException e)
                        {
                            e.printStackTrace();
                        }
                    }
                }
            }
        }
        if (this.cacheDir == null)
        {
            this.cacheDir = new File(appDir.getAbsoluteFile() + "/cache");
        }
        if (!this.cacheDir.exists())
        {
            if (!this.cacheDir.mkdir())
            {
                throw new Exception("Cache did not exist yet and could not get created!");
            }
        }

        this.initDataDir();
    }

    public void initDataDir() throws Exception
    {

    }

    @After
    public final void clean() throws Exception
    {
        this.cleanDataDir();

        File cache = this.cacheDir;
        if (cache.isDirectory() && "cache".equals(cache.getName()))
        {
            try
            {
                Files.walkFileTree(cache.toPath(), new CacheFileDeleter());
            }
            catch (IOException e)
            {
                LOG.warn(e.getLocalizedMessage(), e);
            }
        }
    }

    public void cleanDataDir() throws Exception
    {

    }
}
