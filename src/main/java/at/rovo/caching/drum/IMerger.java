package at.rovo.caching.drum;

/**
 * <p><code>IMerger</code> iterates through all registered {@link IDiskWriter}
 * instances and uses their bucket disk files to extract data to merge. It 
 * therefore only merges data that was created since the last merge. New
 * disk writer objects can be added on invoking {@link #addDiskFileWriter(IDiskWriter)}</p>
 * <p>The merge, by default, is done in the context of the {@link Thread} the
 * merger instance was placed in. The execution thread blocks until it is 
 * signaled about availability of data, which is done via {@link #doMerge()}.</p>
 * <p>To execute the merge in the context of the caller (f.e. the main-thread)
 * use {@link #forceMerge()}.</p>
 * <p>As <code>IMerger</code> administers a data store file in the back, this
 * file needs to be closed after usage to prevent data-loss.</p>
 * 
 * @author Roman Vottner
 */
public interface IMerger<V extends ByteSerializer<V>, A extends ByteSerializer<A>> 
	extends Runnable, IDrumRuntimeListener
{
	/**
	 * <p>Adds a disk writer object to the merger instance, which is used to 
	 * share a lock on the disk file both objects try to access. This is 
	 * necessary as the disk writer could write additional data to the file 
	 * while the merger is reading from the same file, which could result in an 
	 * inconsistent state.</p>
	 * 
	 * @param writer
	 */
	public void addDiskFileWriter(IDiskWriter<V,A> writer);
	
	/**
	 * <p>Signals the merging implementation to start the merging process.</p>
	 * <p>Note that execution of the merge happens in the context of the {@link 
	 * Thread} the merger was placed in while {@link #forceMerge()} is executed
	 * in the main-thread.</p>
	 */
	public void doMerge();
	
	/**
	 * <p>Forces a merge which is done in the {@link Thread} context of the 
	 * caller.</p>
	 * <p>This method differs from {@link #doMerge()} in that <code>doMerge()
	 * </code> invokes execution of the merge in the context of the thread 
	 * created for the {@link IMerger} instance while this method runs in the 
	 * context of the main-thread.</p>
	 */
	public void forceMerge();
	
	/**
	 * <p>Returns the number of unique entries stored into the data store.</p>
	 */
	public long getNumberUniqueEntriesStored();
}
