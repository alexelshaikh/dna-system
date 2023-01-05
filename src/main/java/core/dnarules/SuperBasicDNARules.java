package core.dnarules;

import core.BaseSequence;
import java.util.List;
import java.util.function.Function;

public class SuperBasicDNARules extends DNARulesCollection {
    public static final int MIN_GC_WINDOW_SIZE = 10;
    public static final Function<BaseSequence, Integer> COMPUTE_GC_WINDOW_SIZE = seq -> Math.min(seq.length(), Math.max(seq.length() >> 2, MIN_GC_WINDOW_SIZE));
    public static final int MAX_HP_LEN = 6;
    public static final float TARGET_GC_CONTENT = 0.5f;

    public static final SuperBasicDNARules INSTANCE = createInstance();

    /**
     * Creates an instance with the super basic DNA rules
     */
    public SuperBasicDNARules() {
        super();
        addOrReplaceRule("GC Error", SuperBasicDNARules::gcError); // e1
        addOrReplaceRule("HP Error", SuperBasicDNARules::hpError); // e2
        addOrReplaceRule("HP Window Error", SuperBasicDNARules::gcWindowError); // e3
    }

    private static SuperBasicDNARules createInstance() {
        SuperBasicDNARules rules = new SuperBasicDNARules();
        rules.unmodifiable();
        return rules;
    }

    public static float gcError(BaseSequence seq) {
        return gcError(seq.gcContent());
    }

    private static float gcError(float gc) {
        float diff = Math.abs(gc - TARGET_GC_CONTENT);
        if (diff <= 0.05f)
            return 0.0f;
        if (diff <= 0.1f)
            return 0.4f;
        if (diff <= 0.2f)
            return 0.8f;

        return 1.0f;
    }

    public static float gcWindowError(BaseSequence seq) {
        List<BaseSequence> kmers = seq.kmers(COMPUTE_GC_WINDOW_SIZE.apply(seq));
        float gc;
        float gcMin = 2.0f;
        float gcMax = -1.0f;
        for (var kmer : kmers) {
            gc = kmer.gcContent();
            if (gc < gcMin)
                gcMin = gc;
            if (gc > gcMax)
                gcMax = gc;
        }
        float diff = gcMax - gcMin;
        return Math.min(1.0f, diff * diff * 5.0f);
    }

    public static float hpError(BaseSequence seq) {
        return hpError(seq, MAX_HP_LEN);
    }

    public static float hpError(BaseSequence seq, int hpThreshold) {
        int[] homopolymerIndexes = seq.indexOfHomopolymersAboveThreshold(hpThreshold);
        float sum = 0;
        int count = 0;
        for(int index : homopolymerIndexes) {
            int hpLen = seq.lengthOfHomopolymerAtIndex(index);
            sum += activate(hpLen);
            count++;
        }
        return sum / Math.max(1, count);
    }

    private static float activate(float error) {
        return 1.0f - (1.0f / (1.0f + (float) Math.exp(error - MAX_HP_LEN)));
    }
}

