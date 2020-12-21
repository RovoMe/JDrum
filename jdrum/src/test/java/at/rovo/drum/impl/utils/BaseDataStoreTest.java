package at.rovo.drum.impl.utils;

import at.rovo.drum.datastore.simple.utils.CacheFileDeleter;

import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.file.Files;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for all data store based unit tests. This abstract class does take responsibility of the creation and
 * deletion of the actual data store files and the initialization of the logger instance.
 *
 * @author Roman Vottner
 */
public abstract class BaseDataStoreTest {

    /**
     * The logger of this class
     */
    private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    private File cacheDir = null;

    @BeforeEach
    public final void init() throws Exception {
        final String appDirPath = System.getProperty("user.dir");
        final File appDir = new File(appDirPath);
        if (appDir.isDirectory()) {
            final String[] items = appDir.list();
            if (items != null) {
                for (String item : items) {
                    if (item.endsWith("cache")) {
                        this.cacheDir = new File(item);
                        if (this.cacheDir.isDirectory() && "cache".equals(this.cacheDir.getName())) {
                            try {
                                Files.walkFileTree(this.cacheDir.toPath(), new CacheFileDeleter());
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                }
            }
        }
        if (this.cacheDir == null) {
            this.cacheDir = new File(appDir.getAbsoluteFile() + "/cache");
        }
        if (!this.cacheDir.exists()) {
            if (!this.cacheDir.mkdir()) {
                throw new Exception("Cache did not exist yet and could not get created!");
            }
        }

        this.initDataDir();
    }

    private void initDataDir() {

    }

    @AfterEach
    public final void clean() {
        this.cleanDataDir();

        File cache = this.cacheDir;
        if (cache.isDirectory() && "cache".equals(cache.getName())) {
            try {
                Files.walkFileTree(cache.toPath(), new CacheFileDeleter());
            } catch (IOException e) {
                LOG.warn(e.getLocalizedMessage(), e);
            }
        }
    }

    private void cleanDataDir() {

    }
}
