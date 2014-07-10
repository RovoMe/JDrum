package at.rovo.caching.drum;

import at.rovo.caching.drum.event.DrumEvent;

/**
 * <p>
 * Provides a contract to be informed on any DRUM events triggered by the DRUM
 * framework.
 * </p>
 * 
 * @author Roman Vottner
 */
public interface DrumListener
{
	/**
	 * <p>
	 * Will be invoked if a new DRUM event arises.
	 * </p>
	 * 
	 * @param event
	 *            The DRUM event that triggered the invocation
	 */
	public void update(DrumEvent<? extends DrumEvent<?>> event);
}
