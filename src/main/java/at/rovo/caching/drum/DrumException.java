package at.rovo.caching.drum;

/**
 * <p>A DrumException marks a failure while caching data either to disk or while 
 * reading from the appropriate disk files or the Berkeley DB used in the back.</p>
 * 
 * @author Roman Vottner
 */
public class DrumException extends RuntimeException
{
	private static final long serialVersionUID = 3116709459268263012L;

	/**
	 * <p>Creates a new instance of a drum exception and sets the error 
	 * String to the provided argument</p>
	 * 
	 * @param msg The error message of this instance
	 */
	public DrumException(String msg)
	{
		super(msg);
	}
	
	/**
	 * <p>Creates a new instance of a drum exception and sets the error 
	 * String to the provided argument and sets the throwing object via the 
	 * specified parameter.</p>
	 * 
	 * @param msg The error message of this instance
	 * @param t The object which threw the exception
	 */
	public DrumException(String msg, Throwable t)
	{
		super(msg, t);
	}
}
