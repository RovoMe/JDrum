package at.rovo.caching.drum.util;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.Semaphore;

/**
 * An immutable file handle to a file and its name in the file system.
 *
 * @author Roman Vottner
 */
public class DiskFileHandle
{
    /** As semaphores can be used from different threads use it here as a lock for getting access to the disk bucket
     * file **/
    private final Semaphore lock = new Semaphore(1);

    /** The name of the key/value file **/
    private final String kvFileName;
    /** The reference to the key/value file **/
    private final RandomAccessFile kvFile;
    /** The name of the auxiliary data file **/
    private final String auxFileName;
    /** The reference to the auxiliary data file **/
    private final RandomAccessFile auxFile;

    /**
     * Creates a new immutable instance for the given <em>file</em> with the specified name.
     *
     * @param drumName
     *         The name without the extension of the file this handle is created for
     * @param bucketId
     *         A reference to the key/value file
     */
    public DiskFileHandle(final String drumName, final int bucketId)
    {
        this.kvFileName = "bucket" + bucketId + ".kv";
        this.auxFileName = "bucket" + bucketId + ".aux";

        // check if the cache sub-directory exists - if not create one
        File cacheDir = new File(System.getProperty("user.dir") + "/cache");
        if (!cacheDir.exists())
        {
            if (!cacheDir.mkdir())
            {
                throw new RuntimeException("No cache directory found and could not initialize one!");
            }
        }
        // check if a sub-directory inside the cache sub-directory exists that has the name of this instance - if not
        // create it
        File drumDir = new File(System.getProperty("user.dir") + "/cache/" + drumName);
        if (!drumDir.exists())
        {
            if (!drumDir.mkdir())
            {
                throw new RuntimeException("No cache data dir found and could not initialize one!");
            }
        }

        try
        {
            this.kvFile = new RandomAccessFile(new File(drumDir, "/" + this.kvFileName), "rw");
            this.auxFile = new RandomAccessFile(new File(drumDir, "/" + this.auxFileName), "rw");
        }
        catch (Exception e)
        {
            throw new RuntimeException("Error creating bucket file!", e);
        }
    }

    /**
     * Returns the file name of the key/value file in the filesystem.
     *
     * @return The name of the key/value file
     */
    public String getKVFileName()
    {
        return this.kvFileName;
    }

    /**
     * Returns the file name of the auxiliary data file in the filesystem.
     *
     * @return The name of auxiliary data file
     */
    public String getAuxFileName()
    {
        return this.auxFileName;
    }

    /**
     * Returns a reference to the key/value file.
     *
     * @return The reference to the key/value file
     */
    public RandomAccessFile getKVFile()
    {
        return this.kvFile;
    }

    /**
     * Returns a reference to the auxiliary data file.
     *
     * @return The reference to the auxiliary data file
     */
    public RandomAccessFile getAuxFile()
    {
        return this.auxFile;
    }

    /**
     * Returns a semaphore in order to gain exclusive access to the files.
     *
     * @return A semaphore to acquire exclusive file access
     */
    public Semaphore getLock()
    {
        return this.lock;
    }

    /**
     * Resets the cursor of the key/value and auxiliary data file back to the start.
     *
     * @throws IOException If the position of the cursor could not reset to the beginning
     */
    public void reset() throws IOException
    {
        this.kvFile.seek(0);
        this.auxFile.seek(0);
    }

    /**
     * Closes the files managed by this instance.
     *
     * @throws IOException If during the close of the managed files an error occurred
     */
    public void close() throws IOException
    {
        IOException caught = null;
        try
        {
            this.kvFile.close();
        }
        catch (IOException ex)
        {
            caught = ex;
        }
        try
        {
            this.auxFile.close();
        }
        catch (IOException ex)
        {
            caught = ex;
        }

        if (caught != null)
        {
            throw caught;
        }
    }
}
