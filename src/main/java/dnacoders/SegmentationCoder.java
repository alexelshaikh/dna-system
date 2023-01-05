package dnacoders;

import core.BaseSequence;
import dnacoders.headercoders.BasicDNAPadder;
import utils.DNAPacker;

public class SegmentationCoder implements Coder<BaseSequence, BaseSequence[]> {
    private static final BaseSequence DELIM = new BaseSequence("AC");
    private static final BaseSequence DELIM_ESC = new BaseSequence("GT");

    private final int splitLen;
    private final int targetLength;
    private final DNACoder<BaseSequence> paddingCoder;

    /**
     * Creates a SegmentationCoder that divides a given DNA sequence into segments, each padded but not permuted.
     * @param targetLength the target length of each segment.
     * @param numGcCorrection the length to pad to each segment.
     */
    public SegmentationCoder(int targetLength, int numGcCorrection) {
        this.targetLength = targetLength;
        this.splitLen = targetLength - DELIM.length() - 1 - numGcCorrection;
        if (splitLen <= 0)
            throw new RuntimeException("targetLength too small");

        this.paddingCoder = new BasicDNAPadder(targetLength, DELIM, DELIM_ESC, (seq, remaining) -> BaseSequence.random(remaining, ((seq.length() + remaining) * 0.5f - seq.gcCount()) / remaining));
    }

    @Override
    public BaseSequence[] encode(BaseSequence seq) {
        int len = seq.length();
        BaseSequence header = DNAPacker.pack(numSegments(len));
        header = paddingCoder.encode(header);
        if (header.length() > targetLength)
            throw new RuntimeException("split length was too small! Consider raising targetLength");
        BaseSequence[] split = seq.splitEvery(splitLen);
        BaseSequence[] result = new BaseSequence[split.length + 1];
        result[0] = header;
        for (int i = 0; i < split.length; i++)
            result[i + 1] = paddingCoder.encode(split[i]);

        return result;
    }

    @Override
    public BaseSequence decode(BaseSequence[] seqs) {
        BaseSequence result = new BaseSequence();
        for (int i = 1; i < seqs.length; i++)
            result.append(paddingCoder.decode(seqs[i]));

        return result;
    }

    /**
     * Calculates the required segments for a given DNA sequence's length.
     * @param len the DNA sequence's length
     * @return the number of segments (excluding the one encoding the number of segments).
     */
    public int numSegments(int len) {
        int d = len / splitLen;
        int rest = len % splitLen;
        return d + (rest != 0? 1 : 0);
    }
}
