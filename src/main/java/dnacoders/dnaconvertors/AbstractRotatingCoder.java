package dnacoders.dnaconvertors;

import core.Base;
import core.BaseSequence;
import dnacoders.DNACoder;
import utils.BitString;
import utils.ByteFrequencyLoader;
import utils.SeqBitStringConverter;
import utils.compression.HuffmanCode;

public abstract class AbstractRotatingCoder implements DNACoder<String> {

    protected HuffmanCode.Bytes huffCode;
    protected int base;

    /**
     * Maps the given mapped DNA sequence and digit to a new DNA base.
     * @param previous the already mapped DNA sequence to this point.
     * @param digit the current digit.
     * @return a new DNA base.
     */
    protected abstract Base rotateToBase(BaseSequence previous, char digit);

    /**
     * Maps the given digit and previously mapped bases to a new DNA base.
     * @param digit the current digit.
     * @param previousBases the previous DNA bases mapped.
     * @return a new DNA base.
     */
    protected abstract Base rotateToBase(char digit, Base ...previousBases);

    /**
     * Maps a given position in a mapped DNA sequence back to a Huffman symbol.
     * @param seq the DNA sequence.
     * @param pos the index to be mapped.
     * @return a Huffman symbol.
     */
    protected abstract char invertToDigit(BaseSequence seq, int pos);

    /**
     * Given the previously mapped DNA bases, this method maps a given position in a mapped DNA sequence back to a Huffman symbol.
     * @param seq the DNA sequence.
     * @param pos the index to be mapped.
     * @param previousBases the previously mapped DNA bases.
     * @return a Huffman symbol.
     */
    protected abstract char invertToDigit(BaseSequence seq, int pos, Base ...previousBases);

    /**
     * Loads the desired static Huffman code for the given base.
     * @param base the Huffman's base (e.g. 2).
     */
    public AbstractRotatingCoder(int base) {
        this(HuffmanCode.Bytes.from(base, ByteFrequencyLoader.loadOrGenerateFreqs()));
    }

    /**
     * Loads the given Huffman code for the given base.
     * @param huffCode the supplied Huffman code.
     */
    public AbstractRotatingCoder(HuffmanCode.Bytes huffCode) {
        this.huffCode = huffCode;
        this.base = huffCode.getBase();
    }

    /**
     * Converts the given BitString to a DNA sequence.
     * @param s the BitString to be converted.
     * @return the DNA sequence.
     */
    public BaseSequence encodeWithBase(BitString s) {
        BaseSequence seq = new BaseSequence();
        for (int i = 0; i < s.length(); i += 2)
            seq.append(rotateToBase(seq, SeqBitStringConverter.bitsToDigit(s.subString(i, i + 2))));

        return seq;
    }

    /**
     * Converts the given DNA sequence to a BitString
     * @param sequence the DNA sequence to be converted.
     * @return the BitString.
     */
    public BitString decodeWithBase(BaseSequence sequence) {
        BitString bs = new BitString();
        for (int i = 0; i < sequence.length(); i++)
           bs.append(SeqBitStringConverter.digitToBits(invertToDigit(sequence, i)));

        return bs;
    }

    /**
     * Encodes the given string to a DNA sequence.
     * @param s the string to be encoded.
     * @return the DNA sequence.
     */
    @Override
    public BaseSequence encode(String s) {
        String digits = huffCode.encode(s);
        BaseSequence seq = new BaseSequence();
        for (char d : digits.toCharArray())
            seq.append(rotateToBase(seq, d));

        return seq;
    }

    /**
     * Decodes the given DNA sequence to a String.
     * @param seq the DNA sequence to be decoded.
     * @return the String.
     */
    @Override
    public String decode(BaseSequence seq) {
        int len = seq.length();
        StringBuilder sb = new StringBuilder(len);
        for (int i = 0; i < len; i++)
            sb.append(invertToDigit(seq, i));

        return huffCode.decodeString(sb.toString());
    }

    /**
     * Returns the static Huffman code for a given base.
     * @param base the Huffman's bases (e.g. 2).
     * @return the Huffman code's instance.
     */
    public static HuffmanCode.Bytes loadStaticHuffCode(int base) {
        return HuffmanCode.Bytes.from(base, ByteFrequencyLoader.loadOrGenerateFreqs());
    }

    /**
     * @return the base used for this instance's Huffman code.
     */
    public int getBase() {
        return base;
    }
}
