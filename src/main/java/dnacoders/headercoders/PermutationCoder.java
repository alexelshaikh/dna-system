package dnacoders.headercoders;

import core.BaseSequence;
import dnacoders.DNACoder;
import utils.DNAPacker;
import utils.FuncUtils;
import java.util.Comparator;
import java.util.function.Function;
import java.util.stream.IntStream;

public class PermutationCoder implements HeaderCoder<Integer> {

    protected final Function<BaseSequence, Float> scoreFunc;
    protected int permsCount;
    protected boolean parallel;
    protected DNAPacker.LengthBase lengthBase;

    /**
     * Creates an instance of PermutationCoder that optimizes a given DNA sequence by applying a number of permutations to it and returning that permuted DNA sequence that maximizes the score.
     * @param parallel true to compute permutations in parallel, and false to compute them sequentially.
     * @param permsCount the number of permutations.
     * @param scoreFunc the score function to maximize.
     */
    public PermutationCoder(boolean parallel, int permsCount, Function<BaseSequence, Float> scoreFunc) {
        this.parallel = parallel;
        this.scoreFunc = scoreFunc;
        this.permsCount = permsCount;
        this.lengthBase = DNAPacker.LengthBase.fromNumber(permsCount);
    }

    @Override
    public BaseSequence encode(BaseSequence seq) {
        long seed = DNACoder.seed(seq);
        IntStream s = IntStream.range(0, permsCount);

        return FuncUtils.stream(s, parallel)
                .mapToObj(i -> {
                    BaseSequence seqPermuted = seq.permute(FuncUtils.getUniformPermutation(seed + i, seq.length()));
                    BaseSequence result = DNAPacker.pack(i, lengthBase);
                    result.append(seqPermuted);
                    return result;
                })
                .peek(seqWithHeader -> seqWithHeader.putProperty("score", scoreFunc.apply(seqWithHeader)))
                .max(Comparator.comparing(seqWithHeader -> seqWithHeader.getProperty("score")))
                .orElseThrow();
    }

    @Override
    public Integer decodeHeader(BaseSequence encoded) {
        return (int) DNAPacker.unpackSingle(encoded);
    }

    @Override
    public BaseSequence extractPayload(BaseSequence encoded) {
        return encoded.window(DNAPacker.getPackedLength(encoded, 1));
    }

    @Override
    public BaseSequence decode(BaseSequence payload, Integer header) {
        return payload.permute(FuncUtils.getUniformPermutation(DNACoder.seed(payload) + header, payload.length()).reverseInPlace());
    }
}
