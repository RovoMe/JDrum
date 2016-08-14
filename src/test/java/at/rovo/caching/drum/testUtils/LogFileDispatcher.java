package at.rovo.caching.drum.testUtils;

import at.rovo.caching.drum.NullDispatcher;
import java.io.Serializable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
    private final static Logger logger = LogManager.getLogger(LogFileDispatcher.class);

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
