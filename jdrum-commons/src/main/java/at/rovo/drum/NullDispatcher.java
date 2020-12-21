package at.rovo.drum;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Serializable;

/**
 * The null dispatcher implements the null object design pattern that offers suitable default do nothing behavior for
 * required methods. Its main purpose is to provide a base class to extend from and only require limited methods
 * overriding.
 *
 * @param <V> The type of the value element
 * @param <A> The type of the auxiliary data element
 * @author Roman Vottner
 */
public class NullDispatcher<V extends Serializable, A extends Serializable> implements Dispatcher<V, A> {

    /**
     * Handles unique key check events
     */
    public void uniqueKeyCheck(@Nonnull final Long key, @Nullable final A aux) {

    }

    /**
     * Handles duplicate key check events
     */
    public void duplicateKeyCheck(@Nonnull final Long key,  @Nullable final V value,  @Nullable final A aux) {

    }

    /**
     * Handles unique key update events
     */
    public void uniqueKeyUpdate(@Nonnull final Long key,  @Nullable final V value,  @Nullable final A aux) {

    }

    /**
     * Handles duplicate key update events
     */
    public void duplicateKeyUpdate(@Nonnull final Long key,  @Nullable final V value,  @Nullable final A aux) {

    }

    /**
     * Handles update events
     */
    public void update(@Nonnull final Long key,  @Nullable final V value,  @Nullable final A aux) {

    }
}
