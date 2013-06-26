package at.rovo.caching.drum;

import at.rovo.caching.drum.event.DrumEventDispatcher;

public class CacheFileStorageFactory<V extends ByteSerializer<V>, A extends ByteSerializer<A>> extends DrumStorageFactory<V,A>
{
	public CacheFileStorageFactory(String drumName, int numBuckets, IDispatcher<V,A> dispatcher, Class<V> valueClass, Class<A> auxClass, DrumEventDispatcher eventDispatcher)
	{
		super(drumName, numBuckets, dispatcher, valueClass, auxClass, eventDispatcher);
	}

	@Override
	protected void create(String drumName, int numBuckets, IDispatcher<V, A> dispatcher, Class<V> valueClass, Class<A> auxClass, DrumEventDispatcher eventDispatcher)
	{
		this.merger = new CacheFileMerger<V,A>(drumName, numBuckets, dispatcher, valueClass, auxClass, eventDispatcher);
	}
}
