package at.rovo.test;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import at.rovo.caching.drum.ByteSerializer;
import at.rovo.caching.drum.NullDispatcher;

public class LogFileDispatcher<V extends ByteSerializer<V>, A extends ByteSerializer<A>> extends NullDispatcher<V,A>
{
	private final static Logger logger = LogManager.getLogger(LogFileDispatcher.class);
	@Override
	public void uniqueKeyUpdate(Long key, V value, A aux) 
	{
		logger.info("UniqueKeyUpdate: "+key+" Data: "+value+" Aux: "+aux);
	}
	
	@Override
	public void duplicateKeyUpdate(Long key, V value, A aux)
	{
		logger.info("DuplicateKeyUpdate: "+key+" Data: "+value+" Aux: "+aux);
	}
}
