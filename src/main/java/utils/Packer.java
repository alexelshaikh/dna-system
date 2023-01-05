package utils;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

public class Packer {
    private Packer() {
    }

    /**
     * The enum for the supported data types.
     */
    public enum Type {
        BYTE("000"),
        SHORT("001"),
        INT("010"),
        LONG("011"),
        FLOAT("100"),
        DOUBLE("101"),
        STRING_8("110"),
        STRING_16("111");
        // add unsigned versions?

        static final int LENGTH           = 3;
        static final int MAX_SIZE_8_BIT   = 255;
        static final int MAX_SIZE_16_BIT  = 65535;

        private final BitString signature;

        Type(String s) {
            this.signature = new BitString(s);
        }

        /**
         * Parses a signature and returns the corresponding Type.
         * @param signature the signature.
         * @return the data type for that signature.
         */
        public static Type parse(BitString signature) {
            if (INT.signature.equals(signature))
                return INT;
            if (SHORT.signature.equals(signature))
                return SHORT;
            if (BYTE.signature.equals(signature))
                return BYTE;
            if (LONG.signature.equals(signature))
                return LONG;
            if (FLOAT.signature.equals(signature))
                return FLOAT;
            if (DOUBLE.signature.equals(signature))
                return DOUBLE;
            if (STRING_8.signature.equals(signature))
                return STRING_8;
            if (STRING_16.signature.equals(signature))
                return STRING_16;
            else {
                throw new RuntimeException("no Type found for signature: " + signature);
            }
        }

        public BitString getSignature() {
            return signature;
        }

        public boolean isNumber() {
            return switch(this) {
                case INT, LONG, FLOAT, DOUBLE, BYTE, SHORT -> true;
                default -> false;
            };
        }

        public static boolean isNumber(BitString sig) {
            return sig != null && !sig.equals(STRING_8.signature) && !sig.equals(STRING_16.signature);
        }

        public boolean isString() {
            return switch(this) {
                case STRING_8, STRING_16 -> true;
                default -> false;
            };
        }

        public static boolean isString(BitString sig) {
            return sig != null && (sig.equals(STRING_8.signature) || sig.equals(STRING_16.signature));
        }
    }

    /**
     * Packs a character sequence into a given BitString and Type.
     * @param cs the character sequence to be packed.
     * @param buff the BitString to pack the character sequence into.
     * @param type the data type to pack by.
     */
    public static void pack(CharSequence cs, BitString buff, Type type) {
        String s = cs.toString();
        switch (type) {
            case BYTE   -> pack(Byte.parseByte(s), buff);
            case SHORT  -> pack(Short.parseShort(s), buff);
            case INT    -> pack(Integer.parseInt(s), buff);
            case LONG   -> pack(Long.parseLong(s), buff);
            case FLOAT  -> pack(Float.parseFloat(s), buff);
            case DOUBLE -> pack(Double.parseDouble(s), buff);
            case STRING_8, STRING_16 -> packString(s, buff);
        }
    }

    /**
     * Packs a String into a given BitString.
     * @param s the String to be packed.
     * @param buff the BitString to be packed into.
     */
    public static void packString(String s, BitString buff) {
        int len = s.length();
        if (len <= Type.MAX_SIZE_8_BIT) {
            buff.append(Type.STRING_8.signature)
                    .append((byte) len)
                    .append(s.getBytes(StandardCharsets.UTF_8));
        }
        else if (len <= Type.MAX_SIZE_16_BIT) {
            buff.append(Type.STRING_16.signature)
                    .append((short) len)
                    .append(s.getBytes(StandardCharsets.UTF_8));
        }
        else {
            throw new RuntimeException("string's length is " + len + " > max allowed length " + Type.MAX_SIZE_16_BIT);
        }
    }

    /**
     * Packs a Number into a given BitString.
     * @param n the Number to be packed.
     * @param buff the BitString to be packed into.
     */
    public static void pack(Number n, BitString buff) {
        if (n instanceof Integer)
            buff.append(Type.INT.signature).append(n.intValue());
        else if (n instanceof Short)
            buff.append(Type.SHORT.signature).append(n.shortValue());
        else if (n instanceof Long)
            buff.append(Type.LONG.signature).append(n.longValue());
        else if (n instanceof Float)
            buff.append(Type.FLOAT.signature).append(n.floatValue());
        else if (n instanceof Double)
            buff.append(Type.DOUBLE.signature).append(n.doubleValue());
        else if (n instanceof Byte)
            buff.append(Type.BYTE.signature).append(n.byteValue());
        else
            throw new RuntimeException("unknown Number n=" + n + " of class " + n.getClass().getSimpleName());
    }

    /**
     * Adds specific padding to a given BitString such that it contains a multiple of BYTE.SIZE bits. This method allows the original BitString (without padding) to be recovered by <Code>withoutBytePadding(BitString)</Code>.
     * @param inputBs the BitString to be padded.
     * @return a new BitString representing the input BitString with padding.
     */
    public static BitString withBytePadding(BitString inputBs) {
        byte paddingSize = (byte) (Byte.SIZE - inputBs.length() % Byte.SIZE);
        return new BitString()
                .append(paddingSize)
                .append(inputBs)
                .append(false, paddingSize);
    }

    /**
     * Removes the padding added by <Code>withBytePadding(BitString)</Code>.
     * @param bs the BitString with padding.
     * @return a new BitString representing the input BitString without padding.
     */
    public static BitString withoutBytePadding(BitString bs) {
        int numBits = bs.toByte(0);
        return bs.subString(Byte.SIZE, bs.length() - numBits);
    }

    /**
     * Unpacks a String at a given position.
     * @param bitString the BitString with the packed value(s).
     * @param i the starting position of the packed value.
     * @return the unpacked value.
     */
    public static UnpackingResult<String> unpackString(BitString bitString, int i) {
        int limit = i + Type.LENGTH;
        return unpackString(bitString, limit, Type.parse(bitString.subString(i, limit)));
    }

    /**
     * Unpacks a Number at a given position.
     * @param bitString the BitString with the packed value(s).
     * @param i the starting position of the packed value.
     * @return the unpacked value.
     */
    public static UnpackingResult<? extends Number> unpackNumber(BitString bitString, int i) {
        int limit = i + Type.LENGTH;
        return unpackNumber(bitString, limit, Type.parse(bitString.subString(i, limit)));
    }

    /**
     * Unpacks a String at a given position and Type.
     * @param bitString the BitString with the packed value(s).
     * @param i the starting position of the packed value.
     * @param strType the Type (a String representing Type) used to unpack.
     * @return the unpacked String.
     */
    public static UnpackingResult<String> unpackString(BitString bitString, int i, Type strType) {
        int payloadIndex;
        int toIndex;
        switch (strType) {
            case STRING_8 -> {
                payloadIndex = i + Byte.SIZE;
                toIndex = payloadIndex + bitString.toInt(i, payloadIndex) * Byte.SIZE;
                return new UnpackingResult<>(new String(bitString.toBytes(payloadIndex, toIndex)), toIndex, Type.STRING_8);
            }
            case STRING_16 -> {
                payloadIndex = i + Short.SIZE;
                toIndex = payloadIndex + bitString.toInt(i, payloadIndex) * Byte.SIZE;
                return new UnpackingResult<>(new String(bitString.toBytes(payloadIndex, toIndex)), toIndex, Type.STRING_16);
            }
            default -> throw new RuntimeException("unknown type " + strType);
        }
    }

    /**
     * Unpacks a Number at a given position and Type.
     * @param bitString the BitString with the packed value(s).
     * @param i the starting position of the packed value.
     * @param numType the Type (a Number representing Type) used to unpack.
     * @return the unpacked Number.
     */
    public static UnpackingResult<? extends Number> unpackNumber(BitString bitString, int i, Type numType) {
        int toIndex;
        switch (numType) {
            case BYTE:
                toIndex = i + Byte.SIZE;
                return new UnpackingResult<>(bitString.toByte(i, toIndex), toIndex, Type.BYTE);
            case INT:
                toIndex = i + Integer.SIZE;
                return new UnpackingResult<>(bitString.toInt(i, toIndex), toIndex, Type.INT);
            case SHORT:
                toIndex = i + Short.SIZE;
                return new UnpackingResult<>(bitString.toShort(i, toIndex), toIndex, Type.SHORT);
            case LONG:
                toIndex = i + Long.SIZE;
                return new UnpackingResult<>(bitString.toLong(i, toIndex), toIndex, Type.LONG);
            case FLOAT:
                return new UnpackingResult<>(bitString.toFloat(i), i + Float.SIZE, Type.FLOAT);
            case DOUBLE:
                return new UnpackingResult<>(bitString.toDouble(i), i + Double.SIZE, Type.DOUBLE);
            default:
                throw new RuntimeException("unknown Number type " + numType);
        }
    }

    /**
     * Unpacks a value at a given position and Type.
     * @param bitString the BitString with the packed value(s).
     * @param i the starting position of the packed value.
     * @return the unpacked value.
     */
    public static UnpackingResult<?> unpackAt(BitString bitString, int i) {
        int payloadIndex = i + Type.LENGTH;
        Type type = Type.parse(bitString.subString(i, payloadIndex));
        if (type.isNumber())
            return unpackNumber(bitString, payloadIndex, type);
        else
            return unpackString(bitString, payloadIndex, type);
    }

    /**
     * Unpacks all values in a given BitString.
     * @param bitString the BitString with the packed value(s).
     * @return the list of unpacked values.
     */
    public static List<UnpackingResult<?>> unpack(BitString bitString) {
        return unpackLazy(bitString).toList();
    }

    /**
     * Unpacks all values in a given BitString lazily.
     * @param bitString the BitString with the packed value(s).
     * @return the lazy stream of unpacked values.
     */
    public static Stream<UnpackingResult<?>> unpackLazy(BitString bitString) {
        return FuncUtils.stream(() -> new Iterator<>() {
            final int len = bitString.length();
            int fromIndex = 0;

            @Override
            public boolean hasNext() {
                return fromIndex < len;
            }
            @Override
            public UnpackingResult<?> next() {
                UnpackingResult<?> unpacked = unpackAt(bitString, fromIndex);
                fromIndex = unpacked.lastExclusiveIndex;
                return unpacked;
            }
        });
    }

    /**
     * The record holdin on to an unpacked value.
     * @param <T> the unpacked value's data type.
     * @param lastExclusiveIndex the last index (inclusive) in the BitString where this value was unpacked.
     */
    public record UnpackingResult<T>(T value, int lastExclusiveIndex, Type type) {

        public boolean isNumber() {
            return type.isNumber();
        }

        public boolean isString() {
            return type.isString();
        }

        public boolean isInt() {
            return type == Type.INT;
        }

        public boolean isShort() {
            return type == Type.SHORT;
        }

        public boolean isLong() {
            return type == Type.LONG;
        }

        public boolean isFloat() {
            return type == Type.FLOAT;
        }

        public boolean isDouble() {
            return type == Type.DOUBLE;
        }

        public boolean isByte() {
            return type == Type.BYTE;
        }

        public T getValue() {
            return value;
        }

        public int getLastExclusiveIndex() {
            return lastExclusiveIndex;
        }

        public Type getType() {
            return type;
        }
    }
}
