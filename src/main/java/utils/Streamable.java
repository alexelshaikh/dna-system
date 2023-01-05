package utils;

import java.util.stream.Stream;

public interface Streamable<T> extends Iterable<T> {
    /**
     * Creates a stream for the underlying Iterable object.
     * @return the stream representing the Iterable of T.
     */
    default Stream<T> stream() {
        return FuncUtils.stream(this);
    }
}
