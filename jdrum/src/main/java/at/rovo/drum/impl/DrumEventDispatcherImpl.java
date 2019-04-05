package at.rovo.drum.impl;

import at.rovo.drum.DrumEventDispatcher;
import at.rovo.drum.DrumListener;
import at.rovo.drum.event.DrumEvent;

import java.lang.invoke.MethodHandles;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This {@link DrumEventDispatcher} takes care of broadcasting internal state changes received via {@link
 * #update(DrumEvent)} to all of its registered listeners. To
 * <p>
 * Any object can register its interest on internal state changes via {@link #addDrumListener(DrumListener)} and remove
 * its interest with {@link #removeDrumListener(DrumListener)}.
 *
 * @author Roman Vottner
 */
public class DrumEventDispatcherImpl implements DrumEventDispatcher {

    /**
     * The logger of this class
     */
    private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    /**
     * The set of registered listener instances which need to be informed on state changes
     */
    private final Set<DrumListener> listeners = new CopyOnWriteArraySet<>();
    /**
     * The queue to add internal events to and to take events from to notify the listeners
     */
    private final BlockingQueue<DrumEvent<? extends DrumEvent<?>>> events = new LinkedBlockingQueue<>();
    /**
     * Indicates if the work of this instance is no longer needed and therefore should stop its work
     */
    private volatile boolean stopRequested = false;
    /**
     * A reference to the thread which is blocking on the {@link BlockingQueue#take()} operation in order to interrupt
     * the blocking method on application shutdown
     */
    private Thread dispatchThread = null;

    /**
     * Creates a new instance.
     */
    public DrumEventDispatcherImpl() {

    }

    @Override
    public void addDrumListener(DrumListener listener) {
        this.listeners.add(listener);
    }

    @Override
    public void removeDrumListener(DrumListener listener) {
        this.listeners.remove(listener);
    }

    @Override
    public void update(DrumEvent<? extends DrumEvent<?>> event) {
        this.events.add(event);
    }

    @Override
    public void run() {
        this.dispatchThread = Thread.currentThread();
        while (!this.stopRequested) {
            try {
                DrumEvent<?> event = this.events.take();
                if (event != null) {
                    this.listeners.forEach(listener -> listener.update(event));
                }
            } catch (InterruptedException e) {
                LOG.trace("Event dispatcher interrupted while waiting for further events to dispatch");
            }
        }
        LOG.info("Event dispatcher thread shut down successfully");
    }

    @Override
    public void stop() {
        this.stopRequested = true;
        if (null != this.dispatchThread) {
            this.dispatchThread.interrupt();
        }
    }
}
