import at.rovo.common.UnitTest;
import at.rovo.common.testsuites.ParallelSuite;
import com.googlecode.junittoolbox.IncludeCategories;
import com.googlecode.junittoolbox.SuiteClasses;
import org.junit.runner.RunWith;

/**
 * Custom JUnit suite which executes all <code>@Category(UnitTest.class)</code> annotated tests contained in
 * subdirectories whose class name ends with <code>Test</code>.
 *
 * @author Roman Vottner
 */
@RunWith(ParallelSuite.class)
@SuiteClasses("**/*Test.class")
@IncludeCategories(UnitTest.class)
public class UnitTestSuite
{
}
