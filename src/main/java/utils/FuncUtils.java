package utils;

import utils.rand.Ranlux;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.BaseStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public final class FuncUtils {
    @FunctionalInterface
    public interface RunnableAttempt {
        void run() throws Exception;
    }

    /**
     * Runs the given runnableAttempt and converts all exceptions to RuntimeExceptions.
     * @param runnableAttempt the RunnableAttempt object.
     */
    public static void safeRun(RunnableAttempt runnableAttempt) {
        try {
            runnableAttempt.run();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @param callable the Callable object.
     * @return the result of the callable object. Converts all exceptions to RuntimeExceptions.
     */
    public static <T> T safeCall(Callable<T> callable) {
        try {
            return callable.call();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @param callable the Callable object.
     * @return the result of the callable object, ignoring all exceptions. Returns null in the case of exceptions.
     */
    public static <T> T superSafeCall(Callable<T> callable) {
        try {
            return callable.call();
        }
        catch (Exception ignored) {
            return null;
        }
    }

    /**
     * Runs the given runnableAttempt and ignores all exceptions.
     * @param runnableAttempt the RunnableAttempt object.
     * @return true if no exception was thrown, and false otherwise.
     */
    public static boolean superSafeRun(RunnableAttempt runnableAttempt) {
        try {
            runnableAttempt.run();
            return true;
        }
        catch (Exception ignored) {
            return false;
        }
    }

    /**
     * Attempts to run rTry. If an exception was thrown, it is ignored and escape is run instead.
     * @param rTry the RunnableAttempt object that is attempted to run.
     * @param escape the other RunnableAttempt object that is run if rTry threw an exception.
     */
    public static void tryOrElse(RunnableAttempt rTry, RunnableAttempt escape) {
        if (!superSafeRun(rTry))
            safeRun(escape);
    }

    /**
     * Attempts to call and return rCallable. If an exception was thrown or the result was null, escapeCallable is called and returned instead.
     * @param rCallable the Callable object that is attempted to be called and returned.
     * @param escapeCallable the other Callable object that is called and returned if rCallable threw an exception or returned null.
     */
    public static <T> T tryOrElse(Callable<T> rCallable, Callable<T> escapeCallable) {
        T r1 = superSafeCall(rCallable);
        return r1 != null ? r1 : safeCall(escapeCallable);
    }

    /**
     * Shuffles the given int array in-place.
     * @param array the supplied array to shuffle.
     */
    public static void shuffle(int[] array) {
        Ranlux rand = new Ranlux();
        int takes = array.length - 1;
        int z;
        for (int i = takes; i > 0; i--) {
            z = rand.choose(0, i);
            int iArray = array[i];
            array[i] = array[z];
            array[z] = iArray;
        }
    }

    /**
     * Shuffles the given list in-place.
     * @param list the supplied list to shuffle.
     */
    public static <T> void shuffle(List<T> list) {
        Ranlux rand = new Ranlux();

        int takes = list.size() - 1;
        int z;
        for (int i = takes; i > 0; i--) {
            z = rand.choose(0, i);
            T iT = list.get(i);
            list.set(i, list.get(z));
            list.set(z, iT);
        }
    }

    /**
     * Shuffles the given generic array in-place.
     * @param array the supplied array to shuffle.
     */
    public static <T> void shuffle(T[] array) {
        Ranlux rand = new Ranlux();
        int takes = array.length - 1;
        int z;
        for (int i = takes; i > 0; i--) {
            z = rand.choose(0, i);
            T iArray = array[i];
            array[i] = array[z];
            array[z] = iArray;
        }
    }

    /**
     * Returns a Permutation (using a seed) with the Fisher-Yates method that is uniform for the given seed and length of sequence.
     * @param seed the seed for the random numbers' generator.
     * @return the Permutation instance.
     */
    public static Permutation getUniformPermutation(long seed, int length) {
        return getUniformPermutation(Ranlux.MAX_LUXURY_LEVEL, seed, length);
    }

    /**
     * Returns a Permutation (using a seed) with the Fisher-Yates method that is uniform for the given seed and length of sequence.
     * @param seed the seed for the random numbers' generator.
     * @param luxuryLevel the luxury level (1,2,3,4) for RandLux. The luxury level 4 is the highest.
     * @return the Permutation instance.
     */
    public static Permutation getUniformPermutation(int luxuryLevel, long seed, int length) {
        Ranlux rand = new Ranlux(luxuryLevel, seed);
        int takes = length - 1;
        int[] swapIndexes = new int[takes * 2];
        int c = 0;
        for (int i = takes; i > 0; i--) {
            swapIndexes[c++] = i;
            swapIndexes[c++] = rand.choose(0, i);
        }

        return new Permutation(swapIndexes);
    }

    /**
     * BLocks until the given future object is ready.
     * @param f the future object.
     * @param <T> the return type.
     * @return the result in that future object.
     */
    public static <T> T await(Future<T> f) {
        return safeCall(f::get);
    }

    /**
     * Converts an iterator object to a respective stream.
     * @param it the input iterator.
     * @return a stream containing the elements of the given iterator.
     */
    public static <T> Stream<T> stream(Iterable<T> it) {
        return StreamSupport.stream(it.spliterator(), false);
    }

    /**
     * @param s the stream.
     * @param parallel specifies whether this stream should be parallel.
     * @return the same stream that is now parallel or sequential depending on the specified boolean.
     */
    public static <S extends BaseStream<?, S>> S stream(S s, boolean parallel) {
        return parallel? s.parallel() : s.sequential();
    }

    /**
     * Reverses a given character sequence.
     * @param s the character sequence.
     * @return a new character sequence that represents the input characters in reverse order.
     */
    public static CharSequence reverseCharSequence(CharSequence s) {
        int len = s.length();
        StringBuilder sb = new StringBuilder(len);
        for (int i = len - 1; i >= 0; i--)
            sb.append(s.charAt(i));

        return sb;
    }

    /**
     * Converts a list of bytes to a byte array.
     * @param data the list of bytes.
     * @return the byte array.
     */
    public static byte[] transformByteListToPrimitive(List<Byte> data) {
        byte[] result = new byte[data.size()];
        for (int i = 0; i < result.length; i++)
            result[i] = data.get(i);

        return result;
    }

    /**
     * Returns a thread pool.
     * @param parallel true to return a work stealing thread pool, and false to return a single-threaded thread pool.
     * @return the thread pool.
     */
    public static ExecutorService pool(boolean parallel) {
        return parallel ? Executors.newWorkStealingPool() : Executors.newSingleThreadExecutor();
    }


    /**
     * @param array the array containing all possible outcomes.
     * @return a random element in array.
     */
    public static <T> T random(T... array) {
        return array[(int) (Math.random() * array.length)];
    }

    /**
     * Counts the number of lines in a given file.
     * @param fileName the file's path.
     * @param buffSize the buffer size used to read this file.
     * @return the number of lines.
     */
    public static int countLinesInFile(String fileName, int buffSize) {
        try (InputStream is = new BufferedInputStream(new FileInputStream(fileName))) {
            byte[] c = new byte[buffSize];

            int readChars = is.read(c);
            if (readChars == -1)
                return 0;

            int count = 0;
            while (readChars == buffSize) {
                for (int i = 0; i < buffSize; i++) {
                    if (c[i] == '\n') {
                        count++;
                    }
                }
                readChars = is.read(c);
            }

            while (readChars != -1) {
                for (int i = 0; i < readChars; i++) {
                    if (c[i] == '\n') {
                        count++;
                    }
                }
                readChars = is.read(c);
            }

            return count + 1;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
