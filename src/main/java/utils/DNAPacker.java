package utils;

import core.Base;
import core.BaseSequence;
import dnacoders.dnaconvertors.AbstractRotatingCoder;
import dnacoders.dnaconvertors.RotatingQuattro;

public class DNAPacker {
    private static final AbstractRotatingCoder rotator = RotatingQuattro.INSTANCE;

    /**
     * The enum for the supported data types.
     */
    public enum LengthBase {
        HALF_BYTE(Byte.SIZE / 2, Base.A),
        BYTE(Byte.SIZE, Base.C),
        SHORT(Short.SIZE, Base.T),
        INT_31(Integer.SIZE, Base.G);

        public final int bitCount;
        public final int baseCount;
        public final Base dnaBase;

        LengthBase(int bitCount, Base dnaBase) {
            this.bitCount = bitCount;
            this.baseCount = bitCount / 2;
            this.dnaBase = dnaBase;
        }

        /**
         * Returns the LengthBase for the required number of bits.
         * @param bitCount the number of bits.
         * @return the LengthBase.
         */
        public static LengthBase from(int bitCount) {
            if(bitCount <= HALF_BYTE.bitCount)
                return HALF_BYTE;
            else if(bitCount <= BYTE.bitCount)
                return BYTE;
            else if(bitCount <= SHORT.bitCount)
                return SHORT;
            else if(bitCount <= INT_31.bitCount)
                return INT_31;

            throw new RuntimeException("BitCount was invalid: " + bitCount);
        }

        /**
         * Returns the LengthBase required to store a given number.
         * @param n the number.
         * @return the LengthBase.
         */
        public static LengthBase fromNumber(int n) {
            if (n < 16)
                return LengthBase.HALF_BYTE;
            if (n < 256)
                return LengthBase.BYTE;
            if (n < 65536)
                return LengthBase.SHORT;
            else
                return LengthBase.INT_31;
        }

        /**
         * Decodes the LengthBase from a DNA sequence. The first DNA base refers to the LengthBase.
         * @param seq the DNA sequence.
         * @return the LengthBase.
         */
        public static LengthBase from(BaseSequence seq) {
            return from(seq.get(0));
        }

        /**
         * Decodes the LengthBase from a DNA base.
         * @param base the DNA base.
         * @return the LengthBase.
         */
        public static LengthBase from(Base base) {
            return switch (base) {
                case A -> LengthBase.HALF_BYTE;
                case C -> LengthBase.BYTE;
                case T -> LengthBase.SHORT;
                case G -> LengthBase.INT_31;
            };
        }

        /**
         * Unpacks the first packed number in a given DNA sequence.
         * @param sequence the DNA sequence.
         * @return the decoded number.
         */
        public long unpackSingle(BaseSequence sequence) {
            BaseSequence encodedValue = sequence.window(1, 1 + baseCount);
            BitString decodedBases = rotator.decodeWithBase(encodedValue);
            return decodedBases.toLong(0, bitCount);
        }
    }

    /**
     * Packs the given numbers to the supplied DNA sequence.
     * @param seq the DNA sequence.
     * @param values the numbers to be packed.
     * @return the DNA sequence with packed numbers to its end.
     */
    public static BaseSequence pack(BaseSequence seq, int... values) {
        for (int value: values)
            pack(seq, value);

        return seq;
    }

    /**
     * Packs the given number to the supplied DNA sequence.
     * @param seq the DNA sequence.
     * @param n the number to be packed.
     * @return the DNA sequence with the packed number to its end.
     */
    public static BaseSequence pack(BaseSequence seq, Number n) {
        BitString bits = new BitString();
        appendMinimal(bits, n, false);
        LengthBase lengthBase = LengthBase.from(bits.length());
        return pack(seq, n, lengthBase);
    }

    /**
     * Packs the given number to the supplied DNA sequence as specified by a LengthBase.
     * @param seq the DNA sequence.
     * @param n the number to be packed.
     * @param lb the LengthBase used to pack the number.
     * @return the DNA sequence with the packed number to its end.
     */
    public static BaseSequence pack(BaseSequence seq, Number n, LengthBase lb) {
        BitString newBits = new BitString();
        appendAndFillTo(newBits, n, lb.bitCount);
        seq.append(lb.dnaBase);
        seq.append(rotator.encodeWithBase(newBits));

        return seq;
    }

    /**
     * Packs the given number to a new DNA sequence as specified by a LengthBase.
     * @param n the number to be packed.
     * @param lb the LengthBase used to pack the number.
     * @return the DNA sequence with the packed number.
     */
    public static BaseSequence pack(Number n, LengthBase lb) {
        return pack(new BaseSequence(), n, lb);
    }

    /**
     * Packs the given number to a new DNA sequence. This method packs the number with as few DNA bases as possible.
     * @param n the number to be packed.
     * @return the DNA sequence with the packed number.
     */
    public static BaseSequence pack(Number n) {
        return pack(new BaseSequence(), n);
    }

    /**
     * Appends the given number to the BitString.
     * @param bs the BitString.
     * @param n the number that is appended to the BitString
     * @param fill true to fill the BitString with leading zeros required for the specified number's data type, and false to append the number without filling leading zeros.
     */
    public static void appendMinimal(BitString bs, Number n, boolean fill) {
        if (n instanceof Byte)
            bs.append(n.byteValue(), fill);
        else if (n instanceof Short)
            bs.append(n.shortValue(), fill);
        else if (n instanceof Integer)
            bs.append(n.intValue(), fill);
        else if (n instanceof Long)
            bs.append(n.longValue(), fill);
        else
            throw new RuntimeException("unsupported number: " + n + " of type: " + n.getClass().getSimpleName());
    }

    /**
     * Appends the given number to the BitString.
     * @param bitString the BitString.
     * @param n the number that is appended to the BitString
     * @param fillToNumBits the number of bits the appended number should have. Fills in leading zeros to achieve fillToNumBits bits in total.
     */
    public static void appendAndFillTo(BitString bitString, Number n, int fillToNumBits) {
        BitString bs = new BitString();
        if (n instanceof Byte)
            bs.append(n.byteValue(), false);
        else if (n instanceof Short)
            bs.append(n.shortValue(), false);
        else if (n instanceof Integer)
            bs.append(n.intValue(), false);
        else if (n instanceof Long)
            bs.append(n.longValue(), false);
        else
            throw new RuntimeException("Floats and Doubles have a static length. This method only supports ints, bytes, shorts, and longs.");

        if(bs.length() > fillToNumBits)
            throw new RuntimeException("number " + n + " requires " + bs.length() + " bits > fillToNumBits(" + fillToNumBits + ")");

        bitString.append(false, fillToNumBits - bs.length()).append(bs);
    }

    /**
     * Unpacks packed values in a given DNA sequence.
     * @param sequence the DNA sequence that contains packed values.
     * @param count the number of packed values to unpack.
     * @return the array of unpacked values.
     */
    public static long[] unpack(BaseSequence sequence, int count) {
        long[] vals = new long[count];
        var index = 0;
        for (int i = 0; i < count; i++) {
            vals[i] = unpackSingle(sequence.window(index));
            index += LengthBase.from(sequence.get(index)).baseCount + 1;
        }
        return vals;
    }

    /**
     * Calculates the total number of DNA bases used to pack packedValueCount values.
     * @param sequence the DNA sequence.
     * @param packedValueCount the number of packed values.
     * @return the total number of DNA bases.
     */
    public static int getPackedLength(BaseSequence sequence, int packedValueCount) {
        var index = 0;
        for (int i = 0; i < packedValueCount; i++) {
            index += LengthBase.from(sequence.get(index)).baseCount + 1;
        }
        return index;
    }

    /**
     * Unpacks a single value from a given DNA sequence.
     * @param sequence the DNA sequence.
     * @return the unpacked value.
     */
    public static long unpackSingle(BaseSequence sequence) {
        var lengthBase = LengthBase.from(sequence);
        return lengthBase.unpackSingle(sequence);
    }
}
