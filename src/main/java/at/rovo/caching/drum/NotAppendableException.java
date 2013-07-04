package at.rovo.caching.drum;

import at.rovo.caching.drum.data.AppendableData;

/**
 * <p>
 * A NotAppendableException marks a failure while trying to append data to
 * existing data which does not implement the {@link AppendableData} interface.
 * </p>
 * 
 * @author Roman Vottner
 */
public class NotAppendableException extends Exception
{
	private static final long serialVersionUID = 5526073000231923485L;

	/**
	 * <p>
	 * Creates a new instance of a not appendable exception and sets the error
	 * String to the provided argument
	 * </p>
	 * 
	 * @param msg
	 *            The error message of this instance
	 */
	public NotAppendableException(String msg)
	{
		super(msg);
	}

	/**
	 * <p>
	 * Creates a new instance of a not appendable exception and sets the error
	 * String to the provided argument and sets the throwing object via the
	 * specified parameter.
	 * </p>
	 * 
	 * @param msg
	 *            The error message of this instance
	 * @param t
	 *            The object which threw the exception
	 */
	public NotAppendableException(String msg, Throwable t)
	{
		super(msg, t);
	}
}