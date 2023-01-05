package dnacoders.attributecoders;

import core.BaseSequence;
import core.Attribute;
import utils.AsymmetricCoder;
import utils.Pair;
import java.util.stream.Stream;

public interface AttributeCoder<F, I, T> extends AsymmetricCoder<Stream<F>, Stream<I>, Stream<T>> {
    char SEGMENTATION_ID_SEPARATOR = '.';
    Stream<T> encodeParallel(Stream<F> f);
    Stream<I> decodeParallel(Stream<T> t);
    Stream<DecodedLine<?>> decodeFromUnordered(Stream<EncodedLine> encodedLines);
    Stream<DecodedLine<?>> decodeFromUnorderedParallel(Stream<EncodedLine> encodedLines);

    class EncodedLine extends Pair<BaseSequence, BaseSequence> {
        public EncodedLine(BaseSequence seq1, BaseSequence seq2) {
            super(seq1, seq2);
        }
    }

    class DecodedLine<M> extends Pair<Attribute<?>, M> {
        public DecodedLine(Attribute<?> attribute, M mapping) {
            super(attribute, mapping);
        }
        public Attribute<?> getAttribute() {
            return t1;
        }
        public M getMapping() {
            return t2;
        }
    }
}
