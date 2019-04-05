package at.rovo.drum.datastore.simple.utils;

import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Map;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Custom Hamcrest matcher used to compare actual data store entries with expected ones.
 *
 * @author Roman Vottner
 */
public class DataStoreMatcher {

    /**
     * The logger of this class
     */
    private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    /**
     * Helper class to encapsulate the comparison state during execution of the <code>matchesSafely</code> method.
     *
     * @param <T> The type of the value object
     */
    private static class Status<T> {

        private enum MismatchReason {
            LENGTH_VIOLATION,
            KEY_MISMATCH,
            VALUE_MISMATCH
        }

        MismatchReason mismatchReason;
        int actualLength;
        Long actualKey;
        T actualValue;

        int expectedLength;
        Long expectedKey;
        T expectedValue;

        int entryNum;
    }

    /**
     * Compares a map representation of a {@link Long} key and {@link T} value with an expected state provided as
     * a list of {@link Pair} elements which contain the {@link Long} key as first argument and the actual {@link T}
     * value as second argument.
     *
     * @param expected A list of contained {@link Pair} elements which encapsulate a {@link Long} key and a {@link T} value
     * @param <T>      The type of the value object
     * @return Return a Hamcrest {@link Matcher} object which will compare the provided elements with an actual one.
     */
    public static <T> Matcher<Map<Long, T>> containsEntries(List<Pair<Long, T>> expected) {
        Status<T> status = new Status<>();

        return new TypeSafeMatcher<>() {
            @Override
            protected boolean matchesSafely(Map<Long, T> actual) {
                try {
                    if (expected.size() != actual.size()) {
                        status.mismatchReason = Status.MismatchReason.LENGTH_VIOLATION;
                        status.actualLength = actual.size();
                        status.expectedLength = expected.size();

                        // the actual length of the expected and actual entries differs
                        return false;
                    }

                    int expectedPos = 0;
                    for (Long key : actual.keySet()) {
                        // check that the n'th expected key matches the m'th actual key
                        if (!expected.get(expectedPos).getFirst().equals(key)) {
                            status.mismatchReason = Status.MismatchReason.KEY_MISMATCH;
                            status.actualKey = key;
                            status.expectedKey = expected.get(expectedPos).getFirst();
                            status.entryNum = expectedPos;

                            return false;
                        }
                        // check that the n'th expected value matches the m'th actual value
                        if (expected.get(expectedPos).getLast() == null && actual.get(key) != null
                                || expected.get(expectedPos).getLast() != null && actual.get(key) != null
                                && !expected.get(expectedPos).getLast().equals(actual.get(key))) {
                            status.mismatchReason = Status.MismatchReason.VALUE_MISMATCH;
                            status.actualValue = actual.get(key);
                            status.expectedValue = expected.get(expectedPos).getLast();
                            status.entryNum = expectedPos;

                            return false;
                        }
                        ++expectedPos;
                    }
                } catch (Exception ex) {
                    LOG.error("Caught exception while comparing the content of the data store", ex);
                    ex.printStackTrace();
                    return false;
                }
                return true;
            }

            @Override
            public void describeTo(Description description) {
                if (Status.MismatchReason.LENGTH_VIOLATION == status.mismatchReason) {
                    description.appendText("data store to contain ").appendValue(status.expectedLength).appendText(" entries ");
                } else if (Status.MismatchReason.KEY_MISMATCH == status.mismatchReason) {
                    description.appendText("data store to contain element with key " + status.expectedKey + " at position " + status.entryNum);
                } else {
                    description.appendText("data store to contain element with value " + status.expectedValue + " at position " + status.entryNum);
                }
            }

            @Override
            protected void describeMismatchSafely(final Map<Long, T> actual, final Description mismatchDescription) {
                if (Status.MismatchReason.LENGTH_VIOLATION == status.mismatchReason) {
                    mismatchDescription.appendText("found ").appendValue(status.actualLength);
                    if (status.expectedLength > status.actualLength) {
                        mismatchDescription.appendText(" only");
                    }
                } else if (Status.MismatchReason.KEY_MISMATCH == status.mismatchReason) {
                    mismatchDescription.appendText("found key " + status.actualKey + " at this position");
                } else {
                    mismatchDescription.appendText("found value " + status.actualValue + " at this position");
                }
            }
        };
    }
}
