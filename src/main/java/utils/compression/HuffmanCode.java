package utils.compression;

import dnacoders.Coder;
import utils.FuncUtils;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class HuffmanCode<T> implements Coder<Iterable<T>, String> {

    protected final Encoding<T> encoding;

    private HuffmanCode(Encoding<T> encoding) {
        this.encoding = encoding;
    }

    /**
     * @param base the Huffman code's base (e.g. 2).
     * @param frequencies the calculated frequencies to use for this Huffman code's instance.
     * @param <S> the type of items that are encoded and decoded.
     * @return the Huffman code for the given base and frequencies.
     */
    public static <S> HuffmanCode<S> of(int base, Map<S, Integer> frequencies) {
        return new HuffmanCode<>(new Encoding<>(base, frequencies));
    }

    /**
     * @param base the Huffman code's base (e.g. 2).
     * @param in the iterable of which the elements' frequencies are calculated.
     * @param <S> the type of items that are encoded and decoded.
     * @return the Huffman code for the given base and elements.
     */
    public static <S> HuffmanCode<S> of(int base, Iterable<S> in) {
        return of(base, FuncUtils.stream(in));
    }

    /**
     * @param base the Huffman code's base (e.g. 2).
     * @param in the array of which the elements' frequencies are calculated.
     * @param <F> the type of items that are encoded and decoded.
     * @return the Huffman code for the given base and elements.
     */
    public static <F> HuffmanCode<F> of(int base, F[] in) {
        return of(base, Arrays.stream(in));
    }

    /**
     * @param base the Huffman code's base (e.g. 2).
     * @param in the stream of which the elements' frequencies are calculated.
     * @param <S> the type of items that are encoded and decoded.
     * @return the Huffman code for the given base and elements.
     */
    public static <S> HuffmanCode<S> of(int base, Stream<S> in) {
        return new HuffmanCode<>(new Encoding<>(base, in.collect(Collectors.toMap(t -> t, t -> 1, Integer::sum))));
    }

    /**
     * encodes each element of the given ts into a string.
     * @param ts the array containing the elements.
     * @return the encoded string.
     */
    public String encode(T[] ts) {
        return encode(Arrays.stream(ts));
    }

    @Override
    public String encode(Iterable<T> ts) {
        return encode(FuncUtils.stream(ts));
    }

    public String encode(Stream<T> ts) {
        return ts.map(t -> encoding.encodingMap.get(t)).collect(Collectors.joining());
    }

    /**
     * encodes a single element into a string.
     * @param t the element to be encoded.
     * @return the encoded string.
     */
    public String encodeItem(T t) {
        return encoding.encodingMap.get(t);
    }

    @Override
    public List<T> decode(String s) {
        int wordLen = 1;
        int i = 0;
        String encodedWord;
        List<T> decoded = new ArrayList<>();
        int len = s.length();
        while (i < len) {
            encodedWord = s.substring(i, i + wordLen);
            T decodedWord = encoding.decodingMap.get(encodedWord);
            if (decodedWord != null) {
                decoded.add(decodedWord);
                i += wordLen;
                wordLen = 1;
            }
            else {
                wordLen++;
            }
        }
        return decoded;
    }


    public int getBase() {
        return encoding.base;
    }
    public Map<T, String> getEncodingMap() {
        return encoding.encodingMap;
    }
    public Map<String, T> getDecodingMap() {
        return encoding.decodingMap;
    }
    public Map<T, Integer> getFrequencies() {
        return encoding.frequencies;
    }


    public static class Bytes extends HuffmanCode<Byte> {
        public Bytes(Encoding<Byte> huffCoder) {
            super(huffCoder);
        }

        public static HuffmanCode.Bytes from(int base, Map<Byte, Integer> frequencies) {
            return new HuffmanCode.Bytes(new Encoding<>(base, frequencies));
        }
        public static HuffmanCode.Bytes from(int base, Iterable<Byte> in) {
            return from(base, FuncUtils.stream(in));
        }
        public static HuffmanCode.Bytes from(int base, Byte[] in) {
            return from(base, Arrays.stream(in));
        }
        public static HuffmanCode.Bytes from(int base, String in) {
            return from(base, in.getBytes(StandardCharsets.UTF_8));
        }
        public static HuffmanCode.Bytes from(int base, byte[] in) {
            Map<Byte, Integer> f = new HashMap<>();
            for (byte b : in)
                f.merge(b, 1, Integer::sum);

            return new HuffmanCode.Bytes(new Encoding<>(base, f));
        }
        public static HuffmanCode.Bytes from(int base, Stream<Byte> in) {
            return new HuffmanCode.Bytes(new Encoding<>(base, in.collect(Collectors.toMap(t -> t, t -> 1, Integer::sum))));
        }

        public String encode(byte[] bs) {
            StringBuilder sb = new StringBuilder();
            for (byte b : bs)
                sb.append(encoding.encodingMap.get(b));

            return sb.toString();
        }

        public String encode(String s) {
            return encode(s.getBytes(StandardCharsets.UTF_8));
        }

        public String decodeString(String s) {
            return new String(FuncUtils.transformByteListToPrimitive(decode(s)), StandardCharsets.UTF_8);
        }
    }

    private static final class Encoding<T> {
        static final int MIN_BASE_NUMBER = 2;
        static final int MAX_BASE_NUMBER = HuffmanSymbol.SYMBOLS.size() - 1;

        final int base;
        final Map<T, Integer> frequencies;
        final PriorityQueue<HeapNode<T>> heap;

        Map<T, String> encodingMap;
        Map<String, T> decodingMap;

        Encoding(int base, Map<T, Integer> frequencies) {
            if (base < MIN_BASE_NUMBER)
                throw new RuntimeException("base: " + base + " must be >= " + MIN_BASE_NUMBER);
            if (base > MAX_BASE_NUMBER)
                throw new RuntimeException("base: " + base + " must be <= " + MAX_BASE_NUMBER);

            this.base = base;
            this.frequencies = frequencies;

            this.heap = new PriorityQueue<>();
            this.encodingMap = new HashMap<>();
            this.decodingMap = new HashMap<>();

            init();
        }

        void init() {
            fillHeap();
            combineHeapNodes();
            createHuffmanCodes();
            asUnmodifiableMaps();
        }

        void asUnmodifiableMaps() {
            this.encodingMap = Collections.unmodifiableMap(encodingMap);
            this.decodingMap = Collections.unmodifiableMap(decodingMap);
        }

        void fillHeap() {
            this.frequencies.forEach((k, v) -> heap.offer(new HeapNode<>(k, v, null)));
        }

        void combineHeapNodes() {
            combineNodesIntoHeap(n0());
            while (heap.size() > 1)
                combineNodesIntoHeap(base);
        }

        void combineNodesIntoHeap(int n) {
            List<HeapNode<T>> nodes = new ArrayList<>(n);
            HeapNode<T> node;
            int sumFrequency = 0;
            for (int i = 0; i < n && heap.size() > 0; i++) {
                node = heap.poll();
                nodes.add(node);
                sumFrequency += node.frequency;
            }
            heap.offer(new HeapNode<>(null, sumFrequency, nodes));
        }

        int n0() {
            if (frequencies.size() <= base)
                return frequencies.size();

            return 2 + ((this.frequencies.size() - 2) % (base - 1));
        }

        void createHuffmanCodes() {
            createHuffmanCodesRecursive(Objects.requireNonNull(heap.poll()), "");
        }

        void createHuffmanCodesRecursive(HeapNode<T> node, String currentHuffCode) {
            if (node.symbol != null) {
                this.encodingMap.put(node.symbol, currentHuffCode);
                this.decodingMap.put(currentHuffCode, node.symbol);
            }

            if (node.nodes == null)
                return;

            int i = 0;
            int size = node.nodes.size();
            while (i < size) {
                createHuffmanCodesRecursive(node.nodes.get(i), currentHuffCode + HuffmanSymbol.of(i));
                i++;
            }
        }

        record HeapNode<T>(T symbol,
                           int frequency,
                           List<HeapNode<T>> nodes) implements Comparable<HeapNode<T>> {

            @Override
            public int compareTo(HeapNode node) {
                return this.frequency - node.frequency;
            }
        }
    }

    public static class HuffmanSymbol {
        private static final List<Character> SYMBOLS;
        static {
            List<Character> HuffmanSymbolsList = new ArrayList<>(Arrays.asList(
                    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
                    'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
                    'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z'));
            char c;
            for (int i = 0; i < 256; i++) {
                c = (char) i;
                if (c >= '0' && c <= '9'
                        || c >= 'A' && c <= 'Z'
                        || c >= 'a' && c <= 'z')
                    continue;

                HuffmanSymbolsList.add(c);
            }
            SYMBOLS = Collections.unmodifiableList(HuffmanSymbolsList);
        }

        public static char of(int intValue) {
            return SYMBOLS.get(intValue);
        }
    }
}
