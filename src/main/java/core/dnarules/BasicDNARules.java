package core.dnarules;

import core.Base;
import core.BaseSequence;

public class BasicDNARules extends SuperBasicDNARules {

    public static final int REPEATABLE_SEQ_NOT_STRICT_SIZE  = 9;
    public static final int REPEATABLE_SEQ_STRICT_SIZE      = 20;

    public static final BasicDNARules INSTANCE = createInstance();

    /**
     * Creates an instance with the basic DNA rules
     */
    public BasicDNARules() {
        super();
        addOrReplaceRule("Microsatellites Run 2 Error", BasicDNARules::microSatellitesRun2Error);
        addOrReplaceRule("Microsatellites Run 3 Error", BasicDNARules::microSatellitesRun3Error);
        addOrReplaceRule("Repeatable Region Error(unstrict)", seq -> BasicDNARules.repeatableRegionError(seq, REPEATABLE_SEQ_NOT_STRICT_SIZE, false));
        addOrReplaceRule("Repeatable Region Error (strict)", seq -> BasicDNARules.repeatableRegionError(seq, REPEATABLE_SEQ_STRICT_SIZE, true));
    }

    private static BasicDNARules createInstance() {
        BasicDNARules rules = new BasicDNARules();
        rules.unmodifiable();
        return rules;
    }

    public static float microSatellitesRun2Error(BaseSequence seq) {
        float err = 0.0f;
        for (Base b1 : Base.values())
            for (Base b2 : Base.values())
                err += microSatellitesCountsError(seq.countMatches(new BaseSequence(b1, b2), true));

        return err;
    }

    public static float microSatellitesRun3Error(BaseSequence seq) {
        float err = 0.0f;
        for (Base b1 : Base.values())
            for (Base b2 : Base.values())
                for (Base b3 : Base.values())
                    err += microSatellitesCountsError(seq.countMatches(new BaseSequence(b1, b2, b3), true));

        return err;
    }

    protected static float microSatellitesCountsError(int count) {
        float err = 0.0f;

        if (count > 10)
            err += 0.001f;

        if (count > 15)
            err += 0.002f;

        if (count > 20)
            err += 0.003f;

        if (count > 25)
            err += 0.004f;

        if (count > 30)
            err += 0.005f;

        if (count > 35)
            err += 0.006f;

        if (count > 40)
            err += 0.007f;

        if (count > 45)
            err += 0.008f;

        if (count > 50)
            err += 0.009f;

        if (count > 55)
            err += 0.01f;

        if (count > 60)
            err += 0.011f;

        if (count > 65)
            err += 0.012f;

        if (count > 70)
            err += 0.013f;

        if (count > 75)
            err += 0.014f;

        if (count > 80)
            err += 0.015f;

        if (count > 85)
            err += 0.016f;

        if (count > 90)
            err += 0.017f;

        if (count > 95)
            err += 0.018f;

        if (count > 100)
            err += 0.019f;

        return err;
    }

    public static float repeatableRegionError(BaseSequence seq, int size, boolean strict) {
        int hits = 1;
        int len = seq.length();
        for (int startPos = 0; startPos < len; startPos++) {
            int end_pos = startPos + size;
            if (end_pos > len)
                break;

            BaseSequence subSeq = seq.window(startPos, end_pos);
            if (seq.window(startPos + 1, len).countMatches(subSeq, false) > 0) {
                hits += 1;
                if (strict)
                    return 1.0f;
            }
        }

        if (strict)
            return 0.0f;
        if (hits <= 1)
            return 0.0f;

        float f = (float) hits * size / len;
        return f > 0.44f ? 1.0f : 0.5f * f;
    }
}

