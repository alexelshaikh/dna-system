package utils.compression;

import dnacoders.Coder;
import java.util.*;
import java.util.function.*;
import java.util.stream.Collector;

public class DeltaCode implements Coder<List<Integer>, CharSequence> {

    public static final char RANGE_DELIMITER       = ';';
    public static final char SIZE_DELIMITER        = ',';

    public static final Coder<List<Integer>, CharSequence> DEFAULT_RANGE_DELTA_COMPRESSOR = new DeltaCode();

    private final char rangeDelim;
    private final char sizeDelim;

    public DeltaCode(char rangeDelim, char sizeDelim) {
        if (rangeDelim == sizeDelim)
            throw new RuntimeException("rangeDelim (" + rangeDelim + ") == sizeDelim(" + sizeDelim + ")");
        this.rangeDelim = rangeDelim;
        this.sizeDelim = sizeDelim;
    }

    /**
     * Creates a Delta compressor that compresses a list of sorted integers to a string.
     */
    public DeltaCode() {
        this(RANGE_DELIMITER, SIZE_DELIMITER);
    }

    @Override
    public CharSequence encode(List<Integer> sortedInts) {
        StringBuilder sb = new StringBuilder();
        int previousId   = -2;
        int lastRangeEnd = -1;
        boolean first = true;
        int nDelta;
        Range currentRange = new Range();
        for (int id : sortedInts) {
            if (!currentRange.isEmpty() && previousId + 1 != id) {
                nDelta = currentRange.start - Math.max(lastRangeEnd, 0);
                if (!first)
                    nDelta -= 1;
                else
                    first = false;

                sb.append(nDelta);
                lastRangeEnd = currentRange.start;
                if (currentRange.size == 1) {
                    sb.append(rangeDelim);
                }
                else {
                    sb.append(sizeDelim).append(currentRange.size - 2).append(rangeDelim);
                    lastRangeEnd += currentRange.size - 1;
                }
                currentRange.clear();
            }
            currentRange.add(id);

            previousId = id;
        }

        if (!currentRange.isEmpty()) {
            nDelta = currentRange.start - Math.max(lastRangeEnd, 0);
            if (!first)
                nDelta -= 1;
            sb.append(nDelta);
            if (currentRange.size > 1)
                sb.append(sizeDelim).append(currentRange.size - 2);
        }
        return sb.toString();
    }

    @Override
    public List<Integer> decode(CharSequence cs) {
        String s = cs.toString() + rangeDelim;
        int len = s.length();
        StringBuilder nSb = new StringBuilder();
        StringBuilder sizeSb = new StringBuilder();
        List<Integer> result = new ArrayList<>();
        int i = 0;
        int n = 0;
        int rangeSize;
        int delta;
        char c;
        boolean first = true;
        while(i < len) {
            c = s.charAt(i++);
            if (c >= '0' && c <= '9') {
                nSb.append(c);
            }
            else if (c == sizeDelim) {
                n += Integer.parseInt(nSb.toString());
                if (!first)
                    n += 1;
                else
                    first = false;
                result.add(n);
                nSb.setLength(0);
                sizeSb.setLength(0);

                while (i < len) {
                    c = s.charAt(i++);
                    if (c >= '0' && c <= '9')
                        sizeSb.append(c);
                    else
                        break;
                }

                rangeSize = Integer.parseInt(sizeSb.toString()) + 1;
                delta = 0;

                while (delta++ < rangeSize)
                    result.add(n + delta);

                n += rangeSize;
            }
            else {
                n += Integer.parseInt(nSb.toString());
                if (!first)
                    n += 1;
                else
                    first = false;
                result.add(n);
                nSb.setLength(0);
            }
        }
        return result;
    }

    private static class Range {

        static final int EMPTY_START = -1;
        int start;
        int size;

        public Range() {
            clear();
        }

        public void add(int id) {
            if (isEmpty())
                start = id;
            size++;
        }

        public boolean isEmpty() {
            return start == EMPTY_START;
        }

        public void clear() {
            this.start = EMPTY_START;
            this.size = 0;
        }
    }

    public static class RangeCollector implements Collector<CharSequence, List<Integer>, CharSequence> {

        private final Coder<List<Integer>, ? extends CharSequence> coder;
        private final boolean isSorted;

        public RangeCollector(final Coder<List<Integer>, ? extends CharSequence> coder) {
            this(coder, false);
        }

        public RangeCollector(final Coder<List<Integer>, ? extends CharSequence> coder, boolean isSorted) {
            this.coder = coder;
            this.isSorted = isSorted;
        }
        @Override
        public Supplier<List<Integer>> supplier() {
            return ArrayList::new;
        }
        @Override
        public BiConsumer<List<Integer>, CharSequence> accumulator() {
            return (list, num) -> list.add(Integer.parseInt(num.toString()));
        }
        @Override
        public BinaryOperator<List<Integer>> combiner() {
            return (left, right) -> { left.addAll(right); return left; };
        }
        @Override
        public Function<List<Integer>, CharSequence> finisher() {
            return list -> {
                if (!isSorted)
                    Collections.sort(list);

                return coder.encode(list);
            };
        }

        @Override
        public Set<Characteristics> characteristics() {
            return Collections.emptySet();
        }
    }
}
