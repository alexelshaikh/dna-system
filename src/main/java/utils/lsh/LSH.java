package utils.lsh;

import core.BaseSequence;
import utils.FuncUtils;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class LSH {
    private final int k;
    private final int b;
    private final int bandSize;
    private final List<Map<String, Set<BaseSequence>>> bands;
    private final ReadWriteLock[] bandLocks;
    private final PseudoPermutation[] permutations;

    /**
     * Creates a tread-safe LSH instance that supports concurrent insertion and querying
     * @param k the kmer length
     * @param r the number of hash functions (permutations)
     * @param b the number of bands. Note that for r=120 and b=10, the resulting bandSize is 12 i.e. 12 hash functions per band/signature
     */
    public LSH(int k, int r, int b) {
        if (r % b != 0)
            throw new RuntimeException("r must be a multiple of b");
        if (k > 33)
            throw new RuntimeException("this LSH only supports k-mers up to k = 33");

        this.k = k;
        this.b = b;
        this.bandSize = r / b;

        long kMers = (long) Math.pow(4, k);
        this.permutations = Stream.iterate(new PseudoPermutation(kMers, kMers), p -> new PseudoPermutation(kMers, p.getP())).limit(r).toArray(PseudoPermutation[]::new);
        this.bands = Stream.generate((Supplier<Map<String, Set<BaseSequence>>>) HashMap::new).limit(b).toList();
        this.bandLocks = Stream.generate(ReentrantReadWriteLock::new).limit(b).toArray(ReadWriteLock[]::new);
    }

    /**
     * Inserts a given DNA sequence into this LSH instance. This method is not thread-safe.
     * @param seq the DNA sequence to insert.
     */
    public void insert(BaseSequence seq) {
        String[] sigs = signatures(seq);
        for (int band = 0; band < b; band++)
            bands.get(band).computeIfAbsent(sigs[band], k1 -> new HashSet<>()).add(seq);
    }

    public void insertParallel(Iterable<BaseSequence> it) {
        FuncUtils.stream(it).parallel().forEach(this::insertSafe);
    }

    /**
     * Inserts a given BaseSequence into this LSH instance. This method is thread-safe.
     * @param seq the DNA sequence to insert.
     */
    public void insertSafe(BaseSequence seq) {
        var sigs = signatures(seq);
        Lock lock;
        for (int band = 0; band < b; band++) {
            var map = bands.get(band);
            lock = bandLocks[band].writeLock();
            lock.lock();
            map.computeIfAbsent(sigs[band], k1 -> new HashSet<>()).add(seq);
            lock.unlock();
        }
    }

    /**
     * @return the minHash values for the given BaseSequence.
     */
    public long[] minHashes(BaseSequence seq) {
        List<BaseSequence> kmers = seq.kmers(k);
        int size = kmers.size();
        long[] shingles = new long[size];
        for (int i = 0; i < size; i++)
            shingles[i] = kmers.get(i).toBase4();

        PseudoPermutation p;
        long[] minHashes = new long[permutations.length];
        long permHash;
        long minHash;
        for (int i = 0; i < permutations.length; i++) {
            p = permutations[i];
            minHash = Long.MAX_VALUE;
            for (long shingle : shingles) {
                permHash = p.apply(shingle);
                if (permHash == 0L) {
                    minHash = 0L;
                    break;
                }
                if (permHash < minHash)
                    minHash = permHash;
            }
            minHashes[i] = minHash;
        }
        return minHashes;
    }


    /**
     * This method is not thread-safe.
     * @param seq the input DNA sequence.
     * @return the set of similar BaseSequence this LSH instance matches for the input BaseSequence.
     */
    public Set<BaseSequence> similarSeqs(BaseSequence seq) {
        return similarSeqs(seq, Integer.MAX_VALUE);
    }

    /**
     * This method is thread-safe.
     * @param seq the input DNA sequence.
     * @return the set of similar DNA sequences this LSH instance matches for the input DNA sequence.
     */
    public Set<BaseSequence> similarSeqsSafe(BaseSequence seq) {
        return similarSeqsSafe(seq, Integer.MAX_VALUE);
    }

    /**
     * This method is thread-safe.
     * @param seq the input DNA sequence.
     * @param maxCount the maximum number of matches. If maxCount matches are found, this method returns and does not search for more matches.
     * @return the set of similar DNA sequence this LSH instance matches for the input BaseSequence. It will return maxCount matches at most.
     */
    public Set<BaseSequence> similarSeqsSafe(BaseSequence seq, int maxCount) {
        var sigs = signatures(seq);
        Set<BaseSequence> result = new HashSet<>();
        Set<BaseSequence> matches;
        Lock lock;
        for (int band = 0; band < b; band++) {
            var map = bands.get(band);
            lock = bandLocks[band].readLock();
            lock.lock();
            matches = map.get(sigs[band]);
            if (matches != null) {
                result.addAll(matches);
                if (result.size() >= maxCount) {
                    lock.unlock();
                    return result;
                }
            }
            lock.unlock();
        }
        return result;
    }

    /**
     * This method is not thread-safe.
     * @param seq the input DNA sequence.
     * @param maxCount the maximum number of matches. If maxCount matches are found, this method returns and does not search for more matches.
     * @return the set of similar DNA sequence this LSH instance matches for the input BaseSequence. It will return maxCount matches at most.
     */
    public Set<BaseSequence> similarSeqs(BaseSequence seq, int maxCount) {
        var sigs = signatures(seq);
        Set<BaseSequence> result = new HashSet<>();
        Set<BaseSequence> matches;
        for (int band = 0; band < b; band++) {
            var map = bands.get(band);
            matches = map.get(sigs[band]);
            if (matches != null) {
                result.addAll(matches);
                if (result.size() >= maxCount)
                    return result;
            }
        }
        return result;
    }

    /**
     * @param seq the input DNA sequence.
     * @return the signatures of each band for the input DNA sequence.
     */
    public String[] signatures(BaseSequence seq) {
        var minHashes = minHashes(seq);
        String[] sigs = new String[b];
        StringBuilder sb;
        int offset = 0;
        for (int band = 0; band < b; band++) {
            sb = new StringBuilder();
            for (int m = 0; m < bandSize; m++)
                sb.append(minHashes[m + offset]);

            sigs[band] = sb.toString();
            offset += bandSize;
        }
        return sigs;
    }

    public int getK() {
        return k;
    }

    public int getB() {
        return b;
    }

    public int getR() {
        return permutations.length;
    }
}
