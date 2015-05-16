package at.rovo.caching.drum.data;

/**
 * Convenience class to support serializing and de-serialization of null elements.
 *
 * @author Roman Vottner
 */
public class NullSerializer implements ByteSerializer<NullSerializer>
{
    /**
     * Instantiates a new instance which represents a null value.
     */
    public NullSerializer()
    {

    }

    @Override
    public byte[] toBytes()
    {
        return new byte[0];
    }

    @Override
    public NullSerializer readBytes(byte[] data)
    {
        return new NullSerializer();
    }
}
