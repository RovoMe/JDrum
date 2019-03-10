package at.rovo.drum;

/**
 * A position aware abstract base class.
 *
 * @author Roman Vottner
 */
public abstract class PositionAware {

    /**
     * Will keep track of the original position of this data object before sorting to revert the sorting after the
     * key/value data was persisted in the bucket file
     **/
    private int position = 0;

    /**
     * Intended to store the original position in the array managed by {@link Merger} to enable reverting the
     * sorting after storing the key/value pair into the bucket file.
     *
     * @param position The original position for this data object before sorting
     */
    void setPosition(int position) {
        this.position = position;
    }

    /**
     * Returns the original position of this data object before the sorting happened in the <em>requestMerge()</em>
     * method of {@link Merger}.
     *
     * @return The original position of this data object
     */
    int getPosition() {
        return this.position;
    }
}
