package at.rovo.caching.drum.data;

/**
 * Marks an implementing class as being able to append data to its value field.
 *
 * @param <T>
 *         The type of the data the broker manages
 *
 * @author Roman Vottner
 */
public interface AppendableData<T>
{
    /**
     * Appends data to the value field of this object.
     *
     * @param data
     *         The data to append to the value field of this object
     */
    void append(T data);
}
