package at.rovo.drum.datastore.simple.utils;

/**
 * Defines a simple pair class that stores two elements and provides access to both of them via according getter- and
 * setter-methods.
 *
 * @param <A> The type of the first element
 * @param <B> The type of the second element
 * @author Roman Vottner
 */
public class Pair<A, B> {
    private A a = null;
    private B b = null;

    /**
     * Default Constructor
     */
    public Pair() {

    }

    /**
     * Creates a new instance of this class and sets the first element as a, and the second element as b
     *
     * @param a The first element of the pair
     * @param b The second element of the pair
     */
    public Pair(A a, B b) {
        this.a = a;
        this.b = b;
    }

    /**
     * Returns the first element of this pair
     *
     * @return The first element of this pair
     */
    public A getFirst() {
        return a;
    }

    /**
     * Returns the last element of this pair
     *
     * @return The last element of this pair
     */
    public B getLast() {
        return b;
    }

    /**
     * Sets the first element of this pair
     *
     * @param a The first element of this pair
     */
    public void setFirst(A a) {
        this.a = a;
    }

    /**
     * Sets the last element of this pair
     *
     * @param b The last element of this pair
     */
    public void setLast(B b) {
        this.b = b;
    }
}