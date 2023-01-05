package utils;

import java.io.Serializable;
import java.util.*;

public class Permutation implements Serializable, Cloneable {

    private final int[] indexes;
    private boolean isReversed;

    public Permutation(int[] indexes) {
        this(indexes, false);
    }

    public Permutation(List<Pair<Integer, Integer>> perms) {
        this(getIndexes(perms), false);
    }

    private Permutation(int[] indexes, boolean isReversed) {
        if (indexes.length % 2 != 0)
            throw new RuntimeException("invalid permutation's indexes: indexes.length() % 2 != 0");
        this.indexes = indexes;
        this.isReversed = isReversed;
    }

    public Permutation reverse() {
        return new Permutation(indexes, !isReversed);
    }

    public Permutation reverseInPlace() {
        isReversed = !isReversed;
        return this;
    }

    public <T> List<T> apply(List<T> list) {
        return applyInPlace(new ArrayList<>(list));
    }

    public <T> List<T> applyInPlace(List<T> list) {
        if (isReversed) {
            for (int i = indexes.length - 1; i > 0; i -= 2)
                swap(list, indexes[i - 1], indexes[i]);
        }
        else {
            for (int i = 1; i < indexes.length; i += 2)
                swap(list, indexes[i - 1], indexes[i]);
        }

        return list;
    }

    private static <T> void swap(List<T> list, int i, int j) {
        T ti = list.get(i);
        list.set(i, list.get(j));
        list.set(j, ti);
    }

    private static int[] getIndexes(List<Pair<Integer, Integer>> perms) {
        int size = perms.size();
        int[] indexes = new int[size * 2];
        int c = 0;
        for (Pair<Integer, Integer> p : perms) {
            indexes[c++] = p.getT1();
            indexes[c++] = p.getT2();
        }
        return indexes;
    }

    @Override
    public String toString() {
        return "Permutation{" +
                "indexes=" + Arrays.toString(indexes) +
                ", isReversed=" + isReversed +
                '}';
    }

    @Override
    protected Permutation clone() {
        return new Permutation(indexes, isReversed);
    }
}
