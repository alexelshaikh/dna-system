package utils;

import core.Base;
import core.BaseSequence;

public class SeqBitStringConverter {

    public static final BiMap<String, Base> MAPPER;
    static {
        BiMap<String, Base> m = new BiMap<>();
        m.put("00", Base.A);
        m.put("01", Base.T);
        m.put("10", Base.C);
        m.put("11", Base.G);
        MAPPER = m.unmodifiableMap();
    }

    public static BaseSequence transform(String s) {
        if (s.length() % 2 != 0)
            throw new RuntimeException("string's length not even. Padding missing?");

        BaseSequence seq = new BaseSequence();
        int len = s.length();
        int len_1 = len - 1;
        for (int i = 0; i < len_1; i += 2)
            seq.append(MAPPER.get(s.substring(i, i + 2)));

        return seq;
    }

    public static BaseSequence transform(BitString s) {
        return transform(s.toString());
    }

    public static BitString transform(BaseSequence seq) {
        BitString bitString = new BitString();
        for (Base b : seq)
            bitString.append(MAPPER.getReverse(b));

        return bitString;
    }

    public static String transformToString(BaseSequence seq) {
        StringBuilder sb = new StringBuilder();
        for (Base b : seq)
            sb.append(MAPPER.getReverse(b));

        return sb.toString();
    }


    public static BitString digitToBits(char digit) {
        return switch (digit) {
            case '0' -> new BitString("00");
            case '1' -> new BitString("01");
            case '2' -> new BitString("10");
            case '3' -> new BitString("11");
            default -> throw new RuntimeException();
        };
    }

    public static char bitsToDigit(BitString bs) {
        return switch (bs.toString()) {
            case "00" -> '0';
            case "01" -> '1';
            case "10" -> '2';
            case "11" -> '3';
            default -> throw new RuntimeException();
        };
    }

}
