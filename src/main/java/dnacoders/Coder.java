package dnacoders;

import utils.AsymmetricCoder;

public interface Coder<F, T> extends AsymmetricCoder<F, F, T> {

    static <T1, T2, T3> Coder<T1, T3> fuse(Coder<T1, T2> coder1, Coder<T2, T3> coder2) {
        return new Coder<>() {
            @Override
            public T3 encode(T1 t1) {
                return coder2.encode(coder1.encode(t1));
            }
            @Override
            public T1 decode(T3 t3) {
                return coder1.decode(coder2.decode(t3));
            }
        };
    }

    static <T1, T2, T3, T4> Coder<T1, T4> fuse(Coder<T1, T2> coder1, Coder<T2, T3> coder2, Coder<T3, T4> coder3) {
        return new Coder<>() {
            @Override
            public T4 encode(T1 f) {
                return coder3.encode(coder2.encode(coder1.encode(f)));
            }
            @Override
            public T1 decode(T4 t) {
                return coder1.decode(coder2.decode(coder3.decode(t)));
            }
        };
    }
}
