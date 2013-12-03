package at.rovo.caching.drum;

/**
 * <p>
 * A builder creates and initializes an object of a certain type. It is used to
 * reduce the number of object constructors and to give the parameters of the
 * replaced constructors more semantic.
 * </p>
 * <p>
 * After the values have been set within the builder {@link #build()} is invoked
 * to create the instance.
 * </p>
 *
 * @param <T> The type of the object created by the builder
 * 
 * @author Roman Vottner
 */
public interface IBuilder<T>
{
	/**
	 * <p>
	 * Creates a new instance of the type specified for the builder.
	 * </p>
	 * 
	 * @return The created and initialized object
	 * @throws Exception If during creation and initialization an error occurs
	 */
	public T build() throws Exception;
}
