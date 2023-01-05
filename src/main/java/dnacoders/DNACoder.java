package dnacoders;

import core.BaseSequence;

public interface DNACoder<T> extends Coder<T, BaseSequence> {

    static long seed(BaseSequence seq) {
        return seq.histogram().values().stream().reduce(1, (x, y) -> x * y);
    }

    static <T1, T2> DNACoder<T1> fuse(Coder<T1, T2> coder1, DNACoder<T2> coder2) {
        return new DNACoder<>() {
            @Override
            public BaseSequence encode(T1 t1) {
                return coder2.encode(coder1.encode(t1));
            }
            @Override
            public T1 decode(BaseSequence seq) {
                return coder1.decode(coder2.decode(seq));
            }
        };
    }

    static <T1, T2, T3> DNACoder<T1> fuse(Coder<T1, T2> coder1, Coder<T2, T3> coder2, DNACoder<T3> coder3) {
        return new DNACoder<>() {
            @Override
            public BaseSequence encode(T1 t1) {
                return coder3.encode(coder2.encode(coder1.encode(t1)));
            }
            @Override
            public T1 decode(BaseSequence seq) {
                return coder1.decode(coder2.decode(coder3.decode(seq)));
            }
        };
    }
}
