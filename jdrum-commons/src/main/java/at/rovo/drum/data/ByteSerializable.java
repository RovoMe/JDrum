package at.rovo.drum.data;

import java.io.Serializable;

/**
 * Java's standard serialization mechanism does write a lot more bytes on serialization time then the actual state of
 * the object to persist contains. Although Java provides methods such as <code>
 * writeObject(java.io.ObjectOutputStream)</code> and <code>readObject(java.io.ObjectInputStream)</code>, still the
 * standard de/serialization mechanism writes meta data bytes like the class name of the serialized object ans similar.
 * <p>
 * This interface provides a methods to bypass this serialization limitation and persist only the bytes which are really
 * needed.
 *
 * @author Roman Vottner
 * @link http://www.javaworld.com/article/2072752/the-java-serialization-algorithm-revealed.html
 */
public interface ByteSerializable<T extends Serializable> extends Serializable {

    /**
     * Returns internal state of the implementing object as byte array.
     *
     * @return A byte array containing the internal state representation of the implementing object
     */
    byte[] toBytes();

    /**
     * Converts the provided byte array to an instance of the implementing class.
     *
     * @param bytes The bytes to convert to an instance of this class
     * @return An initialized instance of the implementation class
     */
    T readBytes(byte[] bytes);
}
