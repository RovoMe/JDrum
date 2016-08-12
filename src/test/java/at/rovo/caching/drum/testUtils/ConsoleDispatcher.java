package at.rovo.caching.drum.testUtils;

import at.rovo.caching.drum.NullDispatcher;
import java.io.Serializable;

/**
 * The ConsoleDispatcher simply prints out the unique- and duplicate key updates to the console.
 *
 * @param <V>
 *         The type of the value element
 * @param <A>
 *         The type of the auxiliary data element
 *
 * @author Roman Vottner
 */
public class ConsoleDispatcher<V extends Serializable, A extends Serializable> extends NullDispatcher<V, A>
{
    @Override
    public void uniqueKeyUpdate(Long key, V value, A aux)
    {
        System.out.println("UniqueKeyUpdate: " + key + " Data: " + value + " Aux: " + aux);
    }

    @Override
    public void duplicateKeyUpdate(Long key, V value, A aux)
    {
        System.out.println("DuplicateKeyUpdate: " + key + " Data: " + value + " Aux: " + aux);
    }
}
