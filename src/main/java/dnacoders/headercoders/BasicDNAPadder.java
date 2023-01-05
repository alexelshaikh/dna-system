package dnacoders.headercoders;

import core.Base;
import core.BaseSequence;
import dnacoders.GCFiller;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;

public class BasicDNAPadder implements HeaderCoder<Base> {

    public static final BaseSequence DELIM_DEFAULT = new BaseSequence("CCTTCA");
    public static final BaseSequence DELIM_ESC_DEFAULT = new BaseSequence("GACATC");

    public static final Base DELIM_NONE = Base.A;
    public static final Base DELIM_ONLY = Base.C;
    public static final Base[] DELIM_PLUS_FILLER = new Base[] {Base.T, Base.G};

    private final int minTargetLength_1;

    private final BaseSequence delim;
    private final BaseSequence delimEsc;
    private final BiFunction<BaseSequence, Integer, BaseSequence> fillerFunc;

    private final int delimLen;

    /**
     * Creates an instance of BasicDNAPadder that is used to add padding to DNA sequences.
     * @param minTargetLength the minimum length to pad to.
     */
    public BasicDNAPadder(int minTargetLength) {
        this(minTargetLength, DELIM_DEFAULT, DELIM_ESC_DEFAULT);
    }

    /**
     * Creates an instance of BasicDNAPadder that is used to add padding to DNA sequences.
     * @param minTargetLength the minimum length to pad to.
     * @param delim the delimiter DNA sequence.
     * @param delimEsc the delimiter escape DNA sequence.
     */
    public BasicDNAPadder(int minTargetLength, BaseSequence delim, BaseSequence delimEsc) {
        this(minTargetLength, delim, delimEsc, GCFiller::getTrimmedFiller);
    }

    /**
     * Creates an instance of BasicDNAPadder that is used to add padding to DNA sequences.
     * @param minTargetLength the minimum length to pad to.
     * @param delim the delimiter DNA sequence.
     * @param delimEsc the delimiter escape DNA sequence.
     * @param fillerFunc the function that determines the padding DNA sequence.
     */
    public BasicDNAPadder(int minTargetLength, BaseSequence delim, BaseSequence delimEsc, BiFunction<BaseSequence, Integer, BaseSequence> fillerFunc) {
        if (delim.length() != delimEsc.length())
            throw new RuntimeException("delim.length() != delimEsc.length()");
        this.minTargetLength_1 = minTargetLength - 1;
        this.delimLen = delim.length();
        this.delim = delim;
        this.delimEsc = delimEsc;
        this.fillerFunc = fillerFunc;
    }

    @Override
    public BaseSequence encode(BaseSequence s) {
        BaseSequence seq = s.clone();
        int seqLen = seq.length();
        if (seqLen >= minTargetLength_1) {
            return newSeq(
                    DELIM_NONE,
                    seq
            );
        }

        seq.append(delim);
        int remaining = minTargetLength_1 - seq.length();
        if (remaining <= 0) {
            return newSeq(
                    DELIM_ONLY,
                    seq
            );
        }

        BaseSequence filler = fillerFunc.apply(seq, remaining);
        while(filler.contains(delim))
            filler.replaceInPlace(delim, delimEsc);

        seq.append(filler);
        return newSeq(
                seq.gcContent() > 0.55f? DELIM_PLUS_FILLER[0] : DELIM_PLUS_FILLER[1],
                seq
        );
    }

    @Override
    public BaseSequence extractPayload(BaseSequence encoded) {
        return encoded.window(1);
    }

    @Override
    public Base decodeHeader(BaseSequence encoded) {
        return encoded.get(0);
    }

    @Override
    public BaseSequence decode(BaseSequence payload, Base header) {
        if (header == DELIM_NONE)
            return payload;
        if (header == DELIM_ONLY)
            return payload.subSequence(0, payload.length() - delimLen);

        return payload.subSequence(0, payload.lastIndexOf(delim));
    }

    private static BaseSequence newSeq(Base header, BaseSequence seq) {
        List<Base> baseList = new ArrayList<>(1 + seq.length());
        BaseSequence result = new BaseSequence(baseList);
        result.append(header);
        result.append(seq);
        return result;
    }
}
