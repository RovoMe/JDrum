package at.rovo.caching.drum;

import at.rovo.caching.drum.data.ByteSerializer;

/**
 * The null dispatcher implements the null object design pattern that offers suitable default do nothing behavior for
 * required methods. Its main purpose is to provide a base class to extend from and only require limited methods
 * overriding.
 *
 * @param <V>
 *         The type of the value element
 * @param <A>
 *         The type of the auxiliary data element
 *
 * @author Roman Vottner
 */
public class NullDispatcher<V extends ByteSerializer<V>, A extends ByteSerializer<A>> implements Dispatcher<V, A>
{
    /**
     * Handles unique key check events
     */
    public void uniqueKeyCheck(Long key, A aux)
    {

    }

    /**
     * Handles duplicate key check events
     */
    public void duplicateKeyCheck(Long key, V value, A aux)
    {

    }

    /**
     * Handles unique key update events
     */
    public void uniqueKeyUpdate(Long key, V value, A aux)
    {

    }

    /**
     * Handles duplicate key update events
     */
    public void duplicateKeyUpdate(Long key, V value, A aux)
    {

    }

    /**
     * Handles update events
     */
    public void update(Long key, V value, A aux)
    {

    }
}
