package at.rovo.caching.drum.util;

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
     * @param fileName
     *         The name without the extension of the file this handle is created for
     * @param kvFile
     *         A reference to the key/value file
     * @param auxFile
     *         A reference to the auxiliary data file
     */
    public DiskFileHandle(String fileName, RandomAccessFile kvFile, RandomAccessFile auxFile)
    {
        this.kvFileName = fileName + ".kv";
        this.auxFileName = fileName + ".aux";
        this.kvFile = kvFile;
        this.auxFile = auxFile;
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

    public void reset() throws IOException
    {
        this.kvFile.seek(0);
        this.auxFile.seek(0);
    }
}
