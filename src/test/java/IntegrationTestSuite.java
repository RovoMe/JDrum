import at.rovo.common.IntegrationTest;
import at.rovo.common.testsuites.ParallelSuite;
import com.googlecode.junittoolbox.IncludeCategories;
import com.googlecode.junittoolbox.SuiteClasses;
import org.junit.runner.RunWith;

@RunWith(ParallelSuite.class)
@SuiteClasses("**/*Test.class")
@IncludeCategories(IntegrationTest.class)
public class IntegrationTestSuite
{
}
