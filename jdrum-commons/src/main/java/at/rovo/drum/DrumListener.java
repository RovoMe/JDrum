package at.rovo.drum;

import at.rovo.drum.event.DrumEvent;

import javax.annotation.Nonnull;

/**
 * Provides a contract to be informed on any DRUM events triggered by the DRUM framework.
 *
 * @author Roman Vottner
 */
public interface DrumListener {

    /**
     * Will be invoked if a new DRUM event arises.
     *
     * @param event The DRUM event that triggered the invocation
     */
    void update(@Nonnull final DrumEvent<? extends DrumEvent<?>> event);
}
