package at.rovo.caching.drum;

/**
 * Defines valid operations for the caching system.
 *
 * @author Roman Vottner
 */
public enum DrumOperation
{
	/**
	 * Defines that the cache should be checked against a key for its availability.
	 * <p>
	 * If one is already available {@link DrumResult#DUPLICATE_KEY} should be triggered, else {@link
	 * DrumResult#UNIQUE_KEY} has to be returned
	 */
	CHECK('c'),
	/**
	 * Defines that a value for a cached key needs to be updated. If the key is not yet stored in the cache it should be
	 * created instead with the given value. If the key is present it will be replaced by the new entry.
	 */
	UPDATE('u'),
	/**
	 * Marks a certain element to be {@link #CHECK}ed first and then {@link #UPDATE}d afterwards.
	 */
	CHECK_UPDATE('b'),
	/**
	 * Defines that a value for a cached key needs to be updated. If the key is not yet stored in the cache it should be
	 * created instead with the given value. If the key is already present it will append the data of the new data
	 * element to the content of the already stored data entry instead of replacing the entry.
	 */
	APPEND_UPDATE('a');

	private char c;

	DrumOperation(char c)
	{
		this.c = c;
	}

	/**
	 * Returns the token for the respective DRUM operation.
	 *
	 * @return A character identifying the current DRUM operation
	 */
	public char getTokenForOperation()
	{
		return c;
	}

	/**
	 * Returns a DRUM operation for the given token. If the token is unknown a {@link DrumException} will be thrown.
	 *
	 * @param c
	 * 		The character representing the DRUM operation
	 *
	 * @return The matching DRUM operation
	 *
	 * @throws DrumException
	 * 		If the provided token does not match a valid DRUM operation
	 */
	public static DrumOperation fromToken(char c) throws DrumException
	{
		DrumOperation op;
		if (c == 'c')
		{
			op = DrumOperation.CHECK;
		}
		else if (c == 'u')
		{
			op = DrumOperation.UPDATE;
		}
		else if (c == 'b')
		{
			op = DrumOperation.CHECK_UPDATE;
		}
		else if (c == 'a')
		{
			op = DrumOperation.APPEND_UPDATE;
		}
		else
		{
			throw new DrumException("Invalid DRUM operation token received: " + c);
		}
		return op;
	}
}
