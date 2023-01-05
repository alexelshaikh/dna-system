package core;

import utils.Permutation;
import utils.Streamable;
import java.util.*;
import java.util.function.*;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class BaseSequence implements Streamable<Base>, Cloneable {

    public static final Collector<Base, BaseSequence, BaseSequence> COLLECTOR_BASE = new CollectorBaseSeq<>(BaseSequence::new, BaseSequence::append, (seq1, seq2) -> {seq1.append(seq2); return seq1;});

    private List<Base> bases;
    private Map<String, Object> properties;

    /**
     * Creates an empty BaseSequence
     */
    public BaseSequence() {
        this.bases = new ArrayList<>();
    }

    /**
     * Creates a BaseSequence containing a list of provided DNA bases.
     * @param bases the list of DNA bases that will be added to this instance.
     */
    public BaseSequence(List<Base> bases) {
        this.bases = bases;
    }

    /**
     * Creates a BaseSequence containing the bases provided in the string.
     * @param seq the string of DNA bases that will be parsed and added to this instance.
     */
    public BaseSequence(String seq) {
        this();
        append(seq);
    }

    /**
     * Creates a BaseSequence containing the DNA sequences provided.
     * @param seqs the array of BaseSequence that will be each added into this instance.
     */
    public BaseSequence(BaseSequence... seqs) {
        this();
        for (BaseSequence seq : seqs) {
            append(seq);
        }
    }

    /**
     * Creates a BaseSequence containing an array of provided DNA bases.
     * @param bases the array of DNA bases that will be added to this instance.
     */
    public BaseSequence(Base... bases) {
        this();
        for (Base b : bases) {
            append(b);
        }
    }

    /**
     * Computes the complement of this DNA sequence.
     * @return a new DNA sequence representing the complement of this instance.
     */
    public BaseSequence complement() {
        return stream().map(Base::complement).collect(COLLECTOR_BASE);
    }

    /**
     * Inserts a base at the specified index.
     * @param index the index where the base will be inserted.
     * @param b the base that will be inserted.
     */
    public void insert(int index, Base b) {
        this.bases.add(index, b);
    }


    /**
     * Inserts a BaseSequence at the specified index.
     * @param index the index where the BaseSequence will be inserted.
     * @param seq the base that will be inserted.
     */
    public void insert(int index, BaseSequence seq) {
        this.bases.addAll(index, seq.bases);
    }

    /**
     * Appends a character representing a DNA base to this instance.
     * @param b the character representing a DNA base.
     */
    public void append(char b) {
        this.bases.add(Base.valueOfChar(b));
    }

    /**
     * Appends a DNA base to this instance.
     * @param b the DNA base.
     */
    public void append(Base b) {
        this.bases.add(b);
    }

    /**
     * Appends a BaseSequence to this instance.
     * @param seq the BaseSequence.
     */
    public void append(BaseSequence seq) {
        this.bases.addAll(seq.bases);
    }

    /**
     * Appends a CharSequence representing a DNA sequence to this instance.
     * @param charSequence the CharSequence representing a DNA sequence.
     */
    public void append(CharSequence charSequence) {
        int len = charSequence.length();
        for(int i = 0; i < len; i++)
            append(charSequence.charAt(i));
    }

    /**
     * Replaces a DNA base at the given position.
     * @param index the position to set.
     * @param b the DNA base to set.
     */
    public void set(int index, Base b) {
        this.bases.set(index, b);
    }


    /**
     * Puts a property to this instance.
     * @param propertyName the property name.
     * @param value the property's value.
     */
    public <T> BaseSequence putProperty(String propertyName, T value) {
        if (properties == null)
            properties = new HashMap<>();
        properties.put(propertyName, value);
        return this;
    }

    /**
     * @param propertyName the property name.
     * @return the value of the given property.
     */
    public <T> T getProperty(String propertyName) {
        return getProperty(propertyName, () -> null);
    }

    /**
     * @param propertyName the property name.
     * @param orElse is the given property does not exist, will return this instead.
     * @return the value of the given property. If no value exists, returns orElse instead.
     */
    public <T> T getProperty(String propertyName, Supplier<T> orElse) {
        if (properties == null)
            return orElse.get();

        T t = (T) properties.get(propertyName);
        return t != null? t : orElse.get();
    }

    /**
     * @param len the k-mer length.
     * @return the list of k-mers (can contain duplicates).
     */
    public List<BaseSequence> kmers(int len) {
        int thisLen = length();
        if (len > thisLen)
            throw new RuntimeException("cannot create q grams of len " + len + " for seq of len " + thisLen);

        int sizeLimit = 1 + thisLen - len;
        List<BaseSequence> qGrams = new ArrayList<>(sizeLimit);
        for (int i = 0; i < sizeLimit; i++)
            qGrams.add(window(i, i + len));

        return qGrams;
    }

    /**
     * Returns all properties for this instance.
     * @return the map of properties.
     */
    public Map<String, Object> getProperties() {
        return properties;
    }


    /**
     * @return the number of DNA bases in this instance.
     */
    public int length() {
        return this.bases.size();
    }

    public BaseSequence replace(BaseSequence source, BaseSequence target) {
        return clone().replaceInPlace(source, target);
    }

    /**
     * Replaces source with target in this instance in-place.
     * @param source the DNA sequence to be replaced.
     * @param target the DNA sequence that replaces source
     * @return this instance where source is replaced with target.
     */
    public BaseSequence replaceInPlace(BaseSequence source, BaseSequence target) {
        int index = Collections.indexOfSubList(bases, source.bases);
        if (index >= 0) {
            BaseSequence before = index == 0? new BaseSequence() : window(0, index);
            BaseSequence after = window(index + source.length());
            List<Base> basesReplaced = new ArrayList<>(before.length() + target.length() + after.length());
            basesReplaced.addAll(before.bases);
            basesReplaced.addAll(target.bases);
            basesReplaced.addAll(after.bases);
            this.bases = basesReplaced;
        }
        return this;
    }

    /**
     * Divides this instance in n-mers every n bases. If length() % n != 0, then the last split is smaller than n.
     * @param n the length at which the split happens.
     * @return the DNA sequence array containing the splits.
     */
    public BaseSequence[] splitEvery(int n) {
        int length = length();
        if (n > length)
            return new BaseSequence[] {this};

        BaseSequence[] split = new BaseSequence[(int) Math.ceil((double) length / n)];
        int i = 0;
        int start = 0;
        for (int end = n; end <= length;) {
            split[i++] = subSequence(start, end);
            start = end;
            end += n;
        }
        if (split[split.length - 1] == null)
            split[split.length - 1] = subSequence(start, length);

        return split;
    }

    /**
     * @return a new DNA sequence representing the reversed DNA sequence.
     */
    public BaseSequence reverse() {
        int len = bases.size();
        List<Base> reversed = new ArrayList<>(len);
        for (int i = len - 1; i >= 0; i--)
            reversed.add(bases.get(i));

        return new BaseSequence(reversed);
    }

    /**
     * Reverses this instance in-place.
     * @return the reversed DNA sequence.
     */
    public BaseSequence reverseInPlace() {
        int len = length();
        int lastIndex = len - 1;
        int lenHalf = len / 2;
        for (int i = 0; i < lenHalf; i++)
            swap(i, lastIndex - i);

        return this;
    }

    /**
     * Returns the last index it matches a given DNA sequence.
     * @param seq the sequence to search for.
     * @return the last index it matched seq in this instance.
     */
    public int lastIndexOf(BaseSequence seq) {
        return Collections.lastIndexOfSubList(this.bases, seq.bases);
    }


    /**
     * @return an iterator representing the DNA bases of this instance.
     */
    @Override
    public Iterator<Base> iterator() {
        return bases.listIterator();
    }

    /**
     * Swaps the two DNA bases at the given indexes in-place.
     * @param i the first index.
     * @param j the second index.
     */
    public void swap(int i, int j) {
        Base bi = this.bases.get(i);
        this.bases.set(i, this.bases.get(j));
        this.bases.set(j, bi);
    }

    /**
     * @return the absolute number of G and C in this instance.
     */
    public int gcCount() {
        return (int) stream().filter(b -> b == Base.C || b == Base.G).count();
    }

    /**
     * @return a map containing the absolute number of each DNA base.
     */
    public Map<Base, Integer> histogram() {
        return stream().collect(Collectors.toMap(b -> b, b -> 1, Integer::sum));
    }

    /**
     * Returns a new DNA sequence that is a subsequence of this instance.
     * @param i the starting (inclusive) index.
     * @param j the ending (exclusive) index.
     * @return the subsequence at indexes [i..j) of this instance.
     */
    public BaseSequence subSequence(int i, int j) {
        return new BaseSequence(new ArrayList<>(this.bases.subList(i, j)));
    }

    /**
     * Returns a new DNA sequence that is a subsequence of this instance.
     * @param i the starting (inclusive) index.
     * @return the subsequence at indexes [i..length()) of this instance.
     */
    public BaseSequence subSequence(int i) {
        return subSequence(i, length());
    }

    /**
     * Returns an immutable subsequence of this instance.
     * @param i the starting (inclusive) index.
     * @param j the ending (exclusive) index.
     * @return the subsequence at indexes [i..j) of this instance.
     */
    public BaseSequence window(int i, int j) {
        return new BaseSequence(this.bases.subList(i, j));
    }

    /**
     * Returns an immutable subsequence of this instance.
     * @param i the starting (inclusive) index.
     * @return the subsequence at indexes [i..length()) of this instance.
     */
    public BaseSequence window(int i) {
        return new BaseSequence(this.bases.subList(i, length()));
    }

    /**
     * Checks if a given DNA sequence is contained in this instance.
     * @param seq the DNA sequence to search.
     * @return true if seq is found in this instance, and false otherwise.
     */
    public boolean contains(BaseSequence seq) {
        return Collections.indexOfSubList(this.bases, seq.bases) >= 0;
    }

    /**
     * Searches for homopolymers that are longer than a specified threshold.
     * @param threshold the minimum homopolymer length.
     * @return an int array containing the indexes for the found homopolymers that are longer than threshold.
     */
    public int[] indexOfHomopolymersAboveThreshold(int threshold) {
        int limit = length() - threshold;
        IntStream.Builder indexes = IntStream.builder();
        int hpLen;
        int i = 0;
        while (i < limit) {
            hpLen = lengthOfHomopolymerAtIndex(i);
            if (hpLen > threshold)
                indexes.add(i);

            i += hpLen;
        }
        return indexes.build().toArray();
    }

    /**
     * Calculates the length of the homopolymer starting at a specified position.
     * @param index the index of the homopolymer.
     * @return the length of the homopolymer starting at index.
     */
    public int lengthOfHomopolymerAtIndex(int index) {
        int len = length();
        Base hpBase = this.bases.get(index);
        int hpLen = 0;
        while(index < len && this.bases.get(index++) == hpBase)
            hpLen++;

        return hpLen;
    }

    /**
     * Returns a new DNA sequence representing the permuted DNA sequence of this instance.
     * @param p the permutation.
     * @return the permuted DNA sequence.
     */
    public BaseSequence permute(Permutation p) {
        return clone().permuteInPlace(p);
    }

    /**
     * Permutes this instance in-place with a given permutation.
     * @param p the permutation.
     * @return this instance after permutation.
     */
	public BaseSequence permuteInPlace(Permutation p) {
        p.applyInPlace(this.bases);
        return this;
    }

    /**
     * @return the gc content of this instance.
     */
	public float gcContent() {
		return gcContentOf(bases);
    }

    /**
     * @param list the DNA bases list.
     * @return the gc content of the given list.
     */
    private static float gcContentOf(List<Base> list) {
        return (float) list.stream().filter(b -> b == Base.G || b == Base.C).count() / list.size();
    }

    /**
     * Returns the gc content of the specified window.
     * @param i the start (inclusive) index.
     * @param j the end (exclusive) index.
     * @return the gc content of this instance in [i, j).
     */
    public float gcWindow(int i, int j) {
        return gcContentOf(bases.subList(i, Math.min(j, length())));
    }


    /**
     * @param slice the BaseSequence.
     * @param consecutive if set true, then only consecutive repeats of slice will be counted.
     * @return the count of slice in this instance.
     */
    public int countMatches(BaseSequence slice, boolean consecutive) {
        int lenThis = length();
        int sliceLen = slice.length();
        if (lenThis < sliceLen)
            return 0;

        int start = 0;
        int end = sliceLen;
        int count = 0;
        int maxConsecutiveCount = 0;
        int consecutiveCount = 0;

        while (end < lenThis) {
            if (bases.subList(start, end).equals(slice.bases)) {
                count += 1;
                consecutiveCount += 1;
                start += sliceLen;
                end += sliceLen;
            } else {
                consecutiveCount = 0;
                start += 1;
                end += 1;
            }
            maxConsecutiveCount = Math.max(maxConsecutiveCount, consecutiveCount);
        }

        return consecutive? maxConsecutiveCount : count;
    }

    /**
     * @param i the index.
     * @return the DNA base at the specified index.
     */
    public Base get(int i) {
        return this.bases.get(i);
    }


    /**
     * Checks if this instance is equal to o.
     * @param o the other object.
     * @return true, if o and this instance contains the same DNA bases in the same order.
     */
    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null)
            return false;
        if (o instanceof BaseSequence seq)
            return this.bases.equals(seq.bases);
        if (o instanceof String s)
            return length() == s.length() && toString().equals(s);

        return false;
    }

    /**
     * @return a hash value for this instance.
     */
    @Override
    public int hashCode() {
        return this.bases.hashCode();
    }

    /**
     * Generates a random BaseSequence with given length and GC content.
     * @param len the length of the returned BaseSequence.
     * @param gcContent the target GC content.
     * @return the random BaseSequence.
     */
    public static BaseSequence random(int len, double gcContent) {
        return new BaseSequence(Stream.generate(() -> Base.randomGC(gcContent)).limit(len).collect(Collectors.toList()));
    }

    /**
     * Converts the BaseSequence to a string representing the DNA bases.
     * @return a string representation of this instance.
     */
    @Override
    public String toString() {
        return stream().map(Base::name).collect(Collectors.joining());
    }

    /**
     * @return a copy of this instance.
     */
    @Override
    public BaseSequence clone() {
        return new BaseSequence(this);
    }


    /**
     * Converts this BaseSequence to a number in base 4.
     * @return the base 4 representation of this BaseSequence
     */
    public long toBase4() {
        int len = length();
        long id = 0L;
        long order;
        for (int i = 0; i < len; i++) {
            order = switch (this.bases.get(i)) {
                case A -> 0L;
                case C -> 1L;
                case G -> 2L;
                case T -> 3L;
            };
            if (order == 0L)
                continue;
            id += order * Math.pow(4.0d, i);
        }

        return id;
    }

    private static class CollectorBaseSeq<T, A, R> implements Collector<T, A, R> {
        private static final Set<Collector.Characteristics> ID_FINISH = Collections.unmodifiableSet(EnumSet.of(Collector.Characteristics.IDENTITY_FINISH));

        private final Supplier<A>          supplier;
        private final BiConsumer<A, T>     accumulator;
        private final BinaryOperator<A>    combiner;
        private final Function<A, R>       finisher;
        private final Set<Characteristics> characteristics;

        CollectorBaseSeq(Supplier<A> supplier, BiConsumer<A, T> accumulator, BinaryOperator<A> combiner) {
            this.supplier        = supplier;
            this.accumulator     = accumulator;
            this.combiner        = combiner;
            this.finisher        = f -> (R) f;
            this.characteristics = ID_FINISH;
        }
        @Override
        public BiConsumer<A, T> accumulator() {
            return accumulator;
        }
        @Override
        public Supplier<A> supplier() {
            return supplier;
        }
        @Override
        public BinaryOperator<A> combiner() {
            return combiner;
        }
        @Override
        public Function<A, R> finisher() {
            return finisher;
        }
        @Override
        public Set<Characteristics> characteristics() {
            return characteristics;
        }
    }
}
