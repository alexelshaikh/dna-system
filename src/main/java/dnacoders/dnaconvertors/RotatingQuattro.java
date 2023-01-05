package dnacoders.dnaconvertors;

import core.Base;
import core.BaseSequence;
import utils.BiMap;
import utils.Pair;
import utils.compression.HuffmanCode;

public class RotatingQuattro extends AbstractRotatingCoder {

    private final BiMap<Pair<Pair<Base, Base>, Character>, Pair<Pair<Base, Base>, Base>> biMap;

    private static final int BASE         = 4;
    public static final char DIGIT_ZERO   = HuffmanCode.HuffmanSymbol.of(0);
    public static final char DIGIT_ONE    = HuffmanCode.HuffmanSymbol.of(1);
    public static final char DIGIT_TWO    = HuffmanCode.HuffmanSymbol.of(2);
    public static final char DIGIT_THREE  = HuffmanCode.HuffmanSymbol.of(3);


    public static final RotatingQuattro INSTANCE = loadStaticCode();
    private static RotatingQuattro loadStaticCode() {
        return new RotatingQuattro(loadStaticHuffCode(BASE));
    }


    public RotatingQuattro(HuffmanCode.Bytes huffCode) {
        super(huffCode);
        biMap = new BiMap<>();
        putMappings();
    }

    @Override
    protected char invertToDigit(BaseSequence seq, int pos, Base ...previousBases) {
        Base current = seq.get(pos);
        Base lastBase = previousBases[0];
        Base secondLastBase = null;
        if(previousBases.length > 1)
            secondLastBase = previousBases[1];
        Pair<Base, Base> bb = new Pair<>(lastBase, secondLastBase);
        return biMap.getReverse(new Pair<>(bb, current)).getT2();
    }

    @Override
    protected char invertToDigit(BaseSequence seq, int pos) {
        Base lastBase = null;
        Base secondLastBase = null;
        if (pos > 0)
            lastBase = seq.get(pos - 1);
        if (pos > 1)
            secondLastBase = seq.get(pos - 2);
        return invertToDigit(seq, pos, lastBase, secondLastBase);
    }

    @Override
    protected Base rotateToBase(char digit, Base ...previousBases) {
        Pair<Base, Base> bb = new Pair<>(previousBases[0], previousBases[1]);
        return biMap.get(new Pair<>(bb, digit)).getT2();
    }

    @Override
    protected Base rotateToBase(BaseSequence previous, char digit) {
        Base lastBase = null;
        Base beforeLastBase = null;
        int len = previous.length();
        if (len > 0)
            lastBase = previous.get(len - 1);
        if (len > 1)
            beforeLastBase = previous.get(len - 2);
        return rotateToBase(digit, lastBase, beforeLastBase);
    }

    private void putMappings() {
        put(null, null, Base.A, Base.C, Base.T, Base.G);

        put(null, Base.A, Base.A, Base.C, Base.G, Base.T);
        put(null, Base.C, Base.T, Base.A, Base.C, Base.G);
        put(null, Base.G, Base.G, Base.T, Base.A, Base.C);
        put(null, Base.T, Base.T, Base.A, Base.C, Base.G);

        put(Base.A, Base.A, Base.T, Base.G, Base.A, Base.C);
        put(Base.G, Base.A, Base.G, Base.A, Base.C, Base.T);
        put(Base.T, Base.A, Base.C, Base.T, Base.G, Base.A);
        put(Base.C, Base.A, Base.A, Base.C, Base.T, Base.G);

        put(Base.C, Base.C, Base.A, Base.T, Base.C, Base.G);
        put(Base.G, Base.C, Base.T, Base.C, Base.G, Base.A);
        put(Base.T, Base.C, Base.G, Base.A, Base.T, Base.C);
        put(Base.A, Base.C, Base.C, Base.G, Base.A, Base.T);

        put(Base.G, Base.G, Base.T, Base.A, Base.G, Base.C);
        put(Base.C, Base.G, Base.A, Base.G, Base.C, Base.T);
        put(Base.T, Base.G, Base.C, Base.T, Base.A, Base.G);
        put(Base.A, Base.G, Base.G, Base.C, Base.T, Base.A);

        put(Base.T, Base.T, Base.C, Base.T, Base.G, Base.A);
        put(Base.C, Base.T, Base.A, Base.C, Base.T, Base.G);
        put(Base.G, Base.T, Base.G, Base.A, Base.C, Base.T);
        put(Base.A, Base.T, Base.T, Base.G, Base.A, Base.C);
    }

    private void put(Base beforePrevious, Base previous, Base b1, Base b2, Base b3, Base b4) {
        Pair<Base, Base> bb = new Pair<>(previous, beforePrevious);
        biMap.put(new Pair<>(bb, DIGIT_ZERO), new Pair<>(bb, b1));
        biMap.put(new Pair<>(bb, DIGIT_ONE), new Pair<>(bb, b2));
        biMap.put(new Pair<>(bb, DIGIT_TWO), new Pair<>(bb, b3));
        biMap.put(new Pair<>(bb, DIGIT_THREE), new Pair<>(bb, b4));
    }

}
