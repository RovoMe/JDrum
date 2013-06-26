package at.rovo.caching.drum;

public interface IDrumRuntimeListener
{
	/**
	 * <p>Signals the merging thread to stop work and close all open resources.
	 * The thread should give a running process the chance to finish its work 
	 * before closing all resources.</p>
	 */
	public void stop();
}
