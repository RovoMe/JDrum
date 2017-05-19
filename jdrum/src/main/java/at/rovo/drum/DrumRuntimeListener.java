package at.rovo.drum;

/**
 * <em>IDrumRuntimeListener</em> is used by the DRUM framework to be informed if the framework it going to shut down.
 * <p>
 * This interface should not be used by external classes.
 *
 * @author Roman Vottner
 */
public interface DrumRuntimeListener
{
    /**
     * Signals the merging thread to stop work and close all open resources. The thread should give a running process
     * the chance to finish its work before closing all resources.
     */
    void stop();
}
