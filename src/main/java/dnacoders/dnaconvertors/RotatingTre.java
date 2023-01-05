package dnacoders.dnaconvertors;

import core.Base;
import core.BaseSequence;
import utils.compression.HuffmanCode;
import static core.Base.*;

public class RotatingTre extends AbstractRotatingCoder {

    private static final int BASE       = 3;
    public static final char DIGIT_ZERO = HuffmanCode.HuffmanSymbol.of(0);
    public static final char DIGIT_ONE  = HuffmanCode.HuffmanSymbol.of(1);
    public static final char DIGIT_TWO  = HuffmanCode.HuffmanSymbol.of(2);
    public static final RotatingTre INSTANCE = loadStaticCode();

    public RotatingTre() {
        super(BASE);
    }
    public RotatingTre(HuffmanCode.Bytes huffCode) {
        super(huffCode);
    }

    private static RotatingTre loadStaticCode() {
        return new RotatingTre(loadStaticHuffCode(BASE));
    }

    @Override
    protected Base rotateToBase(char digit, Base... previousBases) {
        Base previousBase = previousBases[0];
        if (digit == DIGIT_ZERO) {
            if (isNullOrA(previousBase)) return C;
            return switch (previousBase) {
                case C -> G;
                case G -> T;
                default -> A;
            };
        }
        if (digit == DIGIT_ONE) {
            if (isNullOrA(previousBase)) return G;
            return switch (previousBase) {
                case C -> T;
                case G -> A;
                default -> C;
            };
        }
        if (digit == DIGIT_TWO) {
            if (isNullOrA(previousBase)) return T;
            return switch (previousBase) {
                case C -> A;
                case G -> C;
                default -> G;
            };
        }
        throw new RuntimeException("no rotating base found for digit=" + digit + ", previous base=" + previousBase);
    }

    @Override
    protected Base rotateToBase(BaseSequence previous, char digit) {
        Base lastBase = null;
        int len = previous.length();
        if (len > 0)
            lastBase = previous.get(len - 1);
        return rotateToBase(digit, lastBase);
    }

    @Override
    protected char invertToDigit(BaseSequence seq, int pos, Base ...previousBases) {
        Base current = seq.get(pos);
        Base previousBase = previousBases[0];
        if (pos > 0)
            previousBase = seq.get(pos - 1);

        if (isNullOrA(previousBase)) {
            switch(current) {
                case C: return DIGIT_ZERO;
                case G: return DIGIT_ONE;
                case T: return DIGIT_TWO;
            }
        }
        else {
            switch(previousBase) {
                case C:
                    switch(current) {
                        case G: return DIGIT_ZERO;
                        case T: return DIGIT_ONE;
                        case A: return DIGIT_TWO;
                    }
                case G:
                    switch(current) {
                        case T: return DIGIT_ZERO;
                        case A: return DIGIT_ONE;
                        case C: return DIGIT_TWO;
                    }
                case T:
                    switch(current) {
                        case A: return DIGIT_ZERO;
                        case C: return DIGIT_ONE;
                        case G: return DIGIT_TWO;
                    }
            }
        }
        throw new RuntimeException("no inverted digit found for previous base=" + previousBase + ", current base=" + current);
    }

    @Override
    protected char invertToDigit(BaseSequence seq, int pos) {
        Base previous = null;
        if (pos > 0)
            previous = seq.get(pos - 1);
        return invertToDigit(seq, pos, previous);
    }

    private static boolean isNullOrA(Base b) {
        return b == null || b == Base.A;
    }
}
