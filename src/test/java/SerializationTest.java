import at.rovo.caching.drum.util.DrumUtil;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.is;

public class SerializationTest
{
	@Test
	public void testStringDeSerialization()
	{
		try
		{
			String test = "This String is 76 characters long and will be converted to an array of bytes";
			byte[] array = DrumUtil.serialize(test);

			Assert.assertEquals(76, array.length);

			String ret = DrumUtil.deserialize(array, String.class);

			Assert.assertEquals(test, ret);
		}
		catch (ClassNotFoundException | IOException e)
		{
			e.printStackTrace();
		}
	}

	@Test
	public void testTestClassDeSerialization()
	{
		try
		{
			TestClass test = new TestClass("test");
			byte[] array = DrumUtil.serialize(test);

			TestClass ret = DrumUtil.deserialize(array, TestClass.class);

			Assert.assertEquals(test, ret);
		}
		catch (IOException | ClassNotFoundException e)
		{
			e.printStackTrace();
		}
	}

	@Test
	public void flipIndex() {
		int active = 0;
		int inactive = 1;

		Assert.assertThat(active ^ 1, is(1));
		Assert.assertThat(inactive ^ 1, is(0));
	}
}