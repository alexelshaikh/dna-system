package utils;

/**
 * A coder that is not symmetric, i.e., the encode() function returns T, the decode() function returns I where T != I is possible.
 * @param <F> the type to encode.
 * @param <I> the decoded type.
 * @param <T> the encoded type.
 */
public interface AsymmetricCoder<F, I, T> {
    /**
     * Encodes the input.
     * @param f the input.
     * @return the encoded input.
     */
    T encode(F f);

    /**
     * Decodes the input.
     * @param t the input.
     * @return the decoded input.
     */
    I decode(T t);
}
