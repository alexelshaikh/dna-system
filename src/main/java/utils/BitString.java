package utils;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.BitSet;
import java.util.Iterator;
import java.util.Objects;
import java.util.stream.Collector;

public class BitString implements Appendable, Streamable<Boolean>, CharSequence, Cloneable, Serializable {

    private static final char TRUE  = '1';
    private static final char FALSE = '0';

    private static final int HIGHEST_SET_BIT_BYTE_POS = Byte.SIZE - 1;
    private static final int HIGHEST_SET_BIT_BYTE = 1 << HIGHEST_SET_BIT_BYTE_POS;

    private static final int HIGHEST_SET_BIT_SHORT_POS = Short.SIZE - 1;
    private static final int HIGHEST_SET_BIT_SHORT = 1 << HIGHEST_SET_BIT_SHORT_POS;

    private static final int HIGHEST_SET_BIT_INT_POS = Integer.SIZE - 1;
    private static final int HIGHEST_SET_BIT_INT = 1 << HIGHEST_SET_BIT_INT_POS;

    private static final int HIGHEST_SET_BIT_LONG_POS = Long.SIZE - 1;
    private static final long HIGHEST_SET_BIT_LONG = 1L << HIGHEST_SET_BIT_LONG_POS;

    private final BitSet bits;
    private int length;

    public BitString() {
        this.bits = new BitSet();
        this.length = 0;
    }

    public BitString(byte[] bytes) {
        this();
        append(bytes);
    }


    public BitString(String bits) {
        this();
        for (char c : bits.toCharArray())
            append(c);
    }

    private BitString(BitSet bits, int length) {
        this.bits = bits;
        this.length = length;
    }

    public BitString append(boolean bit) {
        this.bits.set(length++, bit);
        return this;
    }

    public BitString append(boolean bit, int nTimes) {
        while (nTimes-- > 0)
            this.append(bit);

        return this;
    }

    public BitString append(int integer) {
        return append(integer, true);
    }

    public BitString append(long l) {
        return append(l, true);
    }

    public BitString append(double d) {
        return append(ByteBuffer.allocate(Double.BYTES).order(ByteOrder.BIG_ENDIAN).putDouble(d).array());
    }

    public BitString append(float f) {
        return append(ByteBuffer.allocate(Float.BYTES).order(ByteOrder.BIG_ENDIAN).putFloat(f).array());
    }

    public BitString append(short s) {
        return append(s, true);
    }

    public BitString append(int integer, boolean fill) {
        append(integer, fill, true);
        return this;
    }

    @Override
    public BitString append(CharSequence cs) {
        return append(cs, 0, cs.length());
    }

    @Override
    public BitString append(CharSequence cs, int start, int end) {
        checkLimit(end, cs.length());
        checkRangeSize(start, end, cs.length());

        for (int i = start; i < end; i++)
            append(cs.charAt(i));

        return this;
    }

    @Override
    public BitString append(char c) {
        if (c == TRUE)
            append(true);
        else if (c == FALSE)
            append(false);
        else
            throw new RuntimeException("string contains illegal character: " + c);

        return this;
    }

    public BitString append(byte[] bytes) {
        return append(bytes, true);
    }

    public BitString append(byte[] bytes, boolean fill) {
        for (byte b : bytes)
            append(b, fill, false);

        return this;
    }

    public BitString append(byte b) {
        append(b, true, false);
        return this;
    }

    public BitString append(byte b, boolean fill) {
        append(b, fill, false);
        return this;
    }

    private void append(int i, boolean fill, boolean isInt) {
        int highest1 = getHighest1Pos(i, isInt);
        if (fill) {
            int fillCounter = isInt? HIGHEST_SET_BIT_INT_POS : HIGHEST_SET_BIT_BYTE_POS;
            append(false, fillCounter - highest1);
        }
        appendFrom(i, highest1);
    }

    public BitString append(long l, boolean fill) {
        int highest1 = getHighest1Pos(l);
        if (fill)
            append(false, HIGHEST_SET_BIT_LONG_POS - highest1);

        appendFrom(l, highest1);
        return this;
    }

    public BitString append(short s, boolean fill) {
        int highest1 = getHighest1Pos(s);
        if (fill)
            append(false, HIGHEST_SET_BIT_SHORT_POS - highest1);

        appendFrom(s, highest1);
        return this;
    }

    public void clear() {
        this.length = 0;
    }

    private void appendFrom(long l, int index) {
        long mask = 1L << index;
        do {
            append((l & mask) == mask);
            mask >>= 1;
        } while (mask != 0);
    }

    private int getHighest1Pos(int i, boolean isInt) {
        if (i == 0)
            return 0;
        int mask = HIGHEST_SET_BIT_INT;
        int highest1 = HIGHEST_SET_BIT_INT_POS;
        if (!isInt) {
            mask = HIGHEST_SET_BIT_BYTE;
            highest1 = HIGHEST_SET_BIT_BYTE_POS;
        }
        while((i & mask) == 0) {
            highest1--;
            mask >>= 1;
        }
        return highest1;
    }

    private int getHighest1Pos(long l) {
        if (l == 0)
            return 0;
        long mask = HIGHEST_SET_BIT_LONG;
        int highest1 = HIGHEST_SET_BIT_LONG_POS;
        while((l & mask) == 0) {
            highest1--;
            mask >>= 1;
        }
        return highest1;
    }

    private int getHighest1Pos(short s) {
        if (s == 0)
            return 0;
        int mask = HIGHEST_SET_BIT_SHORT;
        int highest1 = HIGHEST_SET_BIT_SHORT_POS;
        while((s & mask) == 0) {
            highest1--;
            mask >>= 1;
        }
        return highest1;
    }

    public boolean get(int i) {
        return this.bits.get(i);
    }

    @Override
    public int length() {
        return length;
    }

    public BitString subString(int from, int to) {
        checkLimit(to, length);
        return new BitString(this.bits.get(from, to), to - from);
    }

    public BitString subString(int from) {
        return subString(from, length);
    }

    public void set(int pos, boolean value) {
        if (pos < length)
            if (value)
                this.bits.set(pos);
            else
                this.bits.clear(pos);
    }

    public void flip(int pos) {
        if (pos < length)
            this.bits.flip(pos);
    }

    public int toInt(int from) {
        return toInt(from, from + Integer.SIZE);
    }

    public long toLong(int from) {
        return toLong(from, from + Long.SIZE);
    }

    public float toFloat(int from) {
        return ByteBuffer.wrap(toBytes(from, from + Float.SIZE)).order(ByteOrder.BIG_ENDIAN).getFloat();
    }

    public double toDouble(int from) {
        return ByteBuffer.wrap(toBytes(from, from + Double.SIZE)).order(ByteOrder.BIG_ENDIAN).getDouble();
    }

    public byte toByte(int from) {
        return toByte(from, from + Byte.SIZE);
    }

    public short toShort(int from) {
        return toShort(from, from + Short.SIZE);
    }

    public byte toByte(int from, int to) {
        checkRangeSize(from, to, Byte.SIZE);
        return (byte) toInt(from, to);
    }
    public short toShort(int from, int to) {
        checkRangeSize(from, to, Short.SIZE);
        return (short) toInt(from, to);
    }

    public int toInt(int from, int to) {
        checkLimit(to, length);
        checkRangeSize(from, to, Integer.SIZE);
        int result = 0;
        int c = 0;
        for (int k = to - 1; k >= from; k--) {
            if (bits.get(k))
                result |= 1 << c;
            c++;
        }
        return result;
    }

    public long toLong(int from, int to) {
        checkLimit(to, length);
        checkRangeSize(from, to, Long.SIZE);
        long result = 0;
        int c = 0;
        for (int k = to - 1; k >= from; k--) {
            if (bits.get(k))
                result |= 1L << c;
            c++;
        }
        return result;
    }

    private static void checkRangeSize(int from, int to, int maxLength) {
        if (to - from > maxLength)
            throw new RuntimeException("to - from (" + (to - from) + " > max_length (" + maxLength + ")");
    }

    private static void checkLimit(int to, int maxLen) {
        if (to > maxLen)
            throw new RuntimeException("to > maxLen");
    }

    public byte[] toBytes() {
        return toBytes(0, length);
    }

    public byte[] toBytes(int from, int to) {
        checkLimit(to, length);
        int numBits = to - from;
        if (numBits % Byte.SIZE != 0)
            throw new RuntimeException("cannot convert BitString to byte array: length % " + Byte.SIZE + " != 0");

        byte[] bytes = new byte[numBits / Byte.SIZE];
        Iterator<Byte> byteIt = byteIterator(from, to);
        int c = 0;
        while(byteIt.hasNext())
            bytes[c++] = byteIt.next();

        return bytes;
    }

    public Iterator<Byte> byteIterator() {
        return byteIterator(0, length);
    }

    public Iterator<Byte> byteIterator(int from, int to) {
        return new Iterator<>() {
            private int current = from;
            @Override
            public boolean hasNext() {
                return current < to;
            }

            @Override
            public Byte next() {
                try {
                    return toByte(current);
                }
                finally {
                    current += Byte.SIZE;
                }
            }
        };
    }

    @Override
    public Iterator<Boolean> iterator() {
        return iterator(0, length);
    }

    public Iterator<Boolean> iterator(int i, int j) {
        return new Iterator<>() {
            private int index = i;
            @Override
            public boolean hasNext() {
                return index < j;
            }

            @Override
            public Boolean next() {
                return bits.get(index++);
            }
        };
    }

    @Override
    public char charAt(int index) {
        return this.bits.get(index)? TRUE : FALSE;
    }

    @Override
    public CharSequence subSequence(int start, int end) {
        return subString(start, end);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o instanceof String)
            return toString().equals(o);
        if (o instanceof BitString bs) {
            return length == bs.length
                    && bits.equals(bs.bits);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(bits, length);
    }

    @Override
    public String toString() {
        return stream()
                .map(b -> b? TRUE : FALSE)
                .collect(Collector.of(
                        () -> new StringBuilder(length),
                        StringBuilder::append,
                        StringBuilder::append,
                        StringBuilder::toString));
    }

    @Override
    protected BitString clone() {
        return new BitString((BitSet) this.bits.clone(), length);
    }
}
