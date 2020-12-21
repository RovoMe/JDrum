package at.rovo.drum;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Serializable;

/**
 * Defines the methods required by the dispatcher to handle responses of the caching system.
 *
 * @param <V> The type of the value element
 * @param <A> The type of the auxiliary data element
 * @author Roman Vottner
 */
public interface Dispatcher<V extends Serializable, A extends Serializable> {

    /**
     * Handles unique key check events
     *
     * @param key The key of the element
     * @param aux The auxiliary data of the element
     */
    void uniqueKeyCheck(@Nonnull final Long key, @Nullable final A aux);

    /**
     * Handles duplicate key check events
     *
     * @param key   The key of the element
     * @param value The value of the element
     * @param aux   The auxiliary data of the element
     */
    void duplicateKeyCheck(@Nonnull final Long key,  @Nullable final V value,  @Nullable final A aux);

    /**
     * Handles unique key update events
     *
     * @param key   The key of the element
     * @param value The value of the element
     * @param aux   The auxiliary data of the element
     */
    void uniqueKeyUpdate(@Nonnull final Long key,  @Nullable final V value,  @Nullable final A aux);

    /**
     * Handles duplicate key update events
     *
     * @param key   The key of the element
     * @param value The value of the element
     * @param aux   The auxiliary data of the element
     */
    void duplicateKeyUpdate(@Nonnull final Long key,  @Nullable final V value,  @Nullable final A aux);

    /**
     * Handles update events
     *
     * @param key   The key of the element
     * @param value The value of the element
     * @param aux   The auxiliary data of the element
     */
    void update(@Nonnull final Long key,  @Nullable final V value,  @Nullable final A aux);
}
