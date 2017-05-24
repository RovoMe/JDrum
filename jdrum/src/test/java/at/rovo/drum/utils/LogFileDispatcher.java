package at.rovo.drum.utils;

import at.rovo.drum.NullDispatcher;
import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Dispatcher implementation which simply logs unique or duplicate key notification to a log file.
 *
 * @param <V>
 *         The type of the value
 * @param <A>
 *         The type of the auxiliary data
 *
 * @author Roman Vottner
 */
public class LogFileDispatcher<V extends Serializable, A extends Serializable> extends NullDispatcher<V, A>
{
    /** The logger instance used to log unique or duplicate key notifications **/
    private final static Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    @Override
    public void uniqueKeyUpdate(Long key, V value, A aux)
    {
        logger.info("UniqueKeyUpdate: {} Data: {} Aux: {}", key, value, aux);
    }

    @Override
    public void duplicateKeyUpdate(Long key, V value, A aux)
    {
        logger.info("DuplicateKeyUpdate: {} Data: {} Aux: {}", key, value, aux);
    }
}
