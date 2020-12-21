package at.rovo.drum.event;

import at.rovo.drum.DrumListener;
import at.rovo.drum.Merger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Informs {@link DrumListener listeners} that the {@link Merger} has changed its internal state. The new state of the
 * {@link Merger} can be retrieved via {@link #getBucketId()}.
 *
 * @author Roman Vottner
 */
public class MergerStateUpdate extends DrumEvent<MergerStateUpdate> {

    /**
     * The bucket ID the event was triggered from
     */
    private Integer bucketId = null;
    /**
     * The new state of the merger
     */
    private final MergerState state;

    /**
     * Initializes a new {@link Merger} state change event.
     *
     * @param drumName The name of the DRUM instance this event was issued from
     * @param state    The new state the merger is in
     */
    public MergerStateUpdate(@Nonnull final String drumName,
                             @Nonnull final MergerState state) {
        super(drumName, MergerStateUpdate.class);
        this.state = state;
    }

    /**
     * Initializes a new {@link Merger} state change event.
     *
     * @param drumName The name of the DRUM instance this event was issued from
     * @param state    The new state the merger is in
     * @param bucketId The identifier of the bucket the event was triggered from
     */
    public MergerStateUpdate(String drumName, MergerState state, int bucketId) {
        super(drumName, MergerStateUpdate.class);
        this.state = state;
        this.bucketId = bucketId;
    }

    /**
     * The current state the {@link Merger} is in.
     *
     * @return The new state of the merger
     */
    @Nonnull
    public MergerState getState() {
        return this.state;
    }

    /**
     * The identifier of the bucket the event was issued from.
     *
     * @return The bucket identifier the event was sent from
     */
    @Nullable
    public Integer getBucketId() {
        return this.bucketId;
    }

    @Override
    @Nonnull
    public String toString() {
        return this.drumName + " - " + this.currentThread.getName() + " - Merger state changed to: " + this.state;
    }
}
