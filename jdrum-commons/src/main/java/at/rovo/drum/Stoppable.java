package at.rovo.drum;

/**
 * <em>Stoppable</em> marks an implementing class as a running process that can be stopped on invoking {@link #stop()}.
 *
 * @author Roman Vottner
 */
public interface Stoppable
{
    /**
     * Signals the running instance to stop work and close all open resources. The object should give a running process
     * the chance to finish its work before closing all resources.
     */
    void stop();
}
