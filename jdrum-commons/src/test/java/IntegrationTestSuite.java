import at.rovo.common.IntegrationTest;
import at.rovo.common.testsuites.ParallelSuite;
import com.googlecode.junittoolbox.IncludeCategories;
import com.googlecode.junittoolbox.SuiteClasses;
import org.junit.runner.RunWith;

/**
 * Custom JUnit suite which executes all <code>@Category(IntegrationTest.class)</code> annotated tests contained in
 * subdirectories whose class name ends with <code>Test</code>.
 *
 * @author Roman Vottner
 */
@RunWith(ParallelSuite.class)
@SuiteClasses("**/*Test.class")
@IncludeCategories(IntegrationTest.class)
public class IntegrationTestSuite
{
}
