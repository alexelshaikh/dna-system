package dnacoders.dnaconvertors;

import core.Base;
import core.BaseSequence;
import dnacoders.DNACoder;
import utils.ByteFrequencyLoader;
import utils.compression.HuffmanCode;
import utils.SeqBitStringConverter;

public class Bin implements DNACoder<String> {

    public static final HuffmanCode.Bytes HUFF_CODE   = HuffmanCode.Bytes.from(2, ByteFrequencyLoader.loadOrGenerateFreqs());
    public static final Bin INSTANCE          = new Bin();


    @Override
    public BaseSequence encode(String s) {
        String c = HUFF_CODE.encode(s);
        BaseSequence seq = new BaseSequence();
        if (c.length() % 2 == 0) {
            seq.append(Base.A);
            seq.append(SeqBitStringConverter.transform(c));
        }
        else {
            BaseSequence coded = SeqBitStringConverter.transform(c.substring(0, c.length() - 1));
            seq.append(c.charAt(c.length() - 1) == '0'? Base.G : Base.C);
            seq.append(coded);
        }
        return seq;
    }

    @Override
    public String decode(BaseSequence seq) {
        Base first = seq.get(0);
        if (first == Base.A)
            return HUFF_CODE.decodeString(SeqBitStringConverter.transformToString(seq.window(1)));
        else if (first == Base.G)
            return HUFF_CODE.decodeString(SeqBitStringConverter.transformToString(seq.window(1)) + '0');
        return HUFF_CODE.decodeString(SeqBitStringConverter.transformToString(seq.window(1)) + '1');
    }
}
