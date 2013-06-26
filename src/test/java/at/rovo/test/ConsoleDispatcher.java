package at.rovo.test;

import at.rovo.caching.drum.ByteSerializer;
import at.rovo.caching.drum.NullDispatcher;

/**
 * <p>The ConsoleDispatcher simply prints out the unique- and duplicate key updates to the console</p>
 *
 * @param <V> The type of the value element
 * @param <A> The type of the auxiliary data element
 * @author Roman Vottner
 */
public class ConsoleDispatcher<V extends ByteSerializer<V>, A extends ByteSerializer<A>> extends NullDispatcher<V,A>
{
	@Override
	public void uniqueKeyUpdate(Long key, V value, A aux) 
	{
		System.out.println("UniqueKeyUpdate: "+key+" Data: "+value+" Aux: "+aux);
	}
	
	@Override
	public void duplicateKeyUpdate(Long key, V value, A aux)
	{
		System.out.println("DuplicateKeyUpdate: "+key+" Data: "+value+" Aux: "+aux);
	}
}
