package at.rovo.caching.drum;

import at.rovo.caching.drum.event.DrumEventDispatcher;

public abstract class DrumStorageFactory<V extends ByteSerializer<V>, A extends ByteSerializer<A>>
{
	protected IMerger<V, A> merger = null;
	
	public DrumStorageFactory(String drumName, int numBuckets, IDispatcher<V,A> dispatcher, Class<V> valueClass, Class<A> auxClass, DrumEventDispatcher eventDispatcher)
	{
		this.create(drumName, numBuckets, dispatcher, valueClass, auxClass, eventDispatcher);
	}
	
	protected abstract void create(String drumName, int numBuckets, IDispatcher<V,A> dispatcher, Class<V> valueClass, Class<A> auxClass, DrumEventDispatcher eventDispatcher);
	
	public IMerger<V,A> getStorage()
	{
		return this.merger;
	}
	
	public static <V extends ByteSerializer<V>, A extends ByteSerializer<A>> DrumStorageFactory<V,A> getDefaultStorageFactory(String drumName, int numBuckets, IDispatcher<V,A> dispatcher, Class<V> valueClass, Class<A> auxClass, DrumEventDispatcher eventDispatcher)
	{
		return new CacheFileStorageFactory<>(drumName, numBuckets, dispatcher, valueClass, auxClass, eventDispatcher);
	
	}
}
