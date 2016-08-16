package at.rovo.caching.drum.util;

import java.io.RandomAccessFile;

/**
 * An immutable file handle to a file and its name in the file system.
 *
 * @author Roman Vottner
 */
public class DiskFileHandle
{
    /** The name of the file in the file system **/
    private final String fileName;
    /** The reference to the actual file **/
    private final RandomAccessFile file;

    /**
     * Creates a new immutable instance for the given <em>file</em> with the specified name.
     *
     * @param fileName
     *         The name of the file within the file system
     * @param file
     *         A reference to the actual file
     */
    public DiskFileHandle(String fileName, RandomAccessFile file)
    {
        this.fileName = fileName;
        this.file = file;
    }

    /**
     * Returns the file name within the file system.
     *
     * @return The name of the file
     */
    public String getFileName()
    {
        return fileName;
    }

    /**
     * Returns a reference to the file this handle was created for
     *
     * @return The reference to the file
     */
    public RandomAccessFile getFile()
    {
        return file;
    }
}
