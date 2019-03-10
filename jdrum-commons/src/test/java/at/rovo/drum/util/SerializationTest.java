package at.rovo.drum.util;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests the serialization and deserialization of objects through the {@link DrumUtils} helper class.
 *
 * @author Roman Vottner
 */
class SerializationTest {

    @Test
    void testStringDeSerialization() {
        try {
            String test = "This String is 76 characters long and will be converted to an array of bytes";
            byte[] array = DrumUtils.serialize(test);

            assertEquals(76, array.length);

            String ret = DrumUtils.deserialize(array, String.class);

            assertEquals(test, ret);
        } catch (ClassNotFoundException | IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    void testTestClassDeSerialization() {
        try {
            TestClass test = new TestClass("test");
            byte[] array = DrumUtils.serialize(test);

            TestClass ret = DrumUtils.deserialize(array, TestClass.class);

            assertEquals(test, ret);
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Test
    void flipIndex() {
        int active = 0;
        int inactive = 1;

        assertThat(active ^ 1, is(1));
        assertThat(inactive ^ 1, is(0));
    }
}