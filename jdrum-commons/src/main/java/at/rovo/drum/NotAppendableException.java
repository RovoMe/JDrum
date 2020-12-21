package at.rovo.drum;

import at.rovo.drum.data.AppendableData;

import javax.annotation.Nonnull;

/**
 * A NotAppendableException marks a failure while trying to append data to existing data which does not implement the
 * {@link AppendableData} interface.
 *
 * @author Roman Vottner
 */
public class NotAppendableException extends Exception {

    private static final long serialVersionUID = 5526073000231923485L;

    /**
     * Creates a new instance of a not appendable exception and sets the error String to the provided argument
     *
     * @param msg The error message of this instance
     */
    public NotAppendableException(@Nonnull final String msg) {
        super(msg);
    }

    /**
     * Creates a new instance of a not appendable exception and sets the error String to the provided argument and sets
     * the throwing object via the specified parameter.
     *
     * @param msg The error message of this instance
     * @param t   The object which threw the exception
     */
    public NotAppendableException(@Nonnull final String msg, @Nonnull final Throwable t) {
        super(msg, t);
    }
}