package dnacoders.attributecoders;

import core.BaseSequence;
import core.Attribute;
import dnacoders.Coder;
import dnacoders.DNACoder;
import dnacoders.SegmentationCoder;
import utils.FuncUtils;
import utils.Pair;
import utils.PooledCompletionService;
import utils.compression.DeltaCode;
import utils.csv.CsvLine;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class NonUniqueAttributeCoder implements AttributeCoder<CsvLine, AttributeCoder.DecodedLine<?>, List<AttributeCoder.EncodedLine>> {

    protected final DNACoder<Attribute<?>> attributeCoder;
    protected final DNACoder<byte[]> byteCoder;
    protected final Coder<List<Integer>,CharSequence> rangeCompressor;
    protected final String keyName;
    protected final String attributeName;
    protected final SegmentationCoder segmentationCoder;

    /**
     * Creates an encoder for a non-unique attribute.
     * @param attributeName the non-unique attribute's name to be encoded.
     * @param keyName the unique attribute's name this encoder will map to.
     * @param attributeCoder the instance to encode attributes.
     * @param byteCoder the instance to encode a byte array (e.g. RQCoder).
     * @param rangeCompressor the delta compressor to compress integers.
     * @param segmentationCoder the instance to apply segmentation.
     */
    public NonUniqueAttributeCoder(String attributeName,
                                   String keyName,
                                   DNACoder<Attribute<?>> attributeCoder,
                                   DNACoder<byte[]> byteCoder,
                                   Coder<List<Integer>, CharSequence> rangeCompressor,
                                   SegmentationCoder segmentationCoder) {

        this.attributeCoder = attributeCoder;
        this.byteCoder = byteCoder;
        this.rangeCompressor = rangeCompressor;
        this.attributeName = attributeName;
        this.keyName = keyName;
        this.segmentationCoder = segmentationCoder;
    }

    protected Stream<List<EncodedLine>> encode(Stream<CsvLine> lines, Collector<CsvLine, ?, ? extends Map<CharSequence, CharSequence>> grouper, boolean parallel) {
        final ExecutorService pool = FuncUtils.pool(parallel);
        final PooledCompletionService<List<EncodedLine>> service = new PooledCompletionService<>(pool);
        final Future<Map<CharSequence, CharSequence>> groups = pool.submit(() -> lines.collect(grouper));
        ExecutorService singleExecutor = Executors.newSingleThreadExecutor();
        singleExecutor.execute(() -> {
            FuncUtils.safeCall(groups::get).forEach((key, value) -> service.submit(() -> {
                String attributeValue = key.toString();
                BaseSequence byteCoded = byteCoder.encode(value.toString().getBytes(StandardCharsets.UTF_8));
                int numPartitions = segmentationCoder.numSegments(byteCoded.length());
                BaseSequence[] attributes = new BaseSequence[numPartitions + 1];

                attributes[0] = attributeCoder.encode(new Attribute<>(attributeName, attributeValue));
                for (int i = 1; i < attributes.length; i++)
                    attributes[i] = attributeCoder.encode(new Attribute<>(attributeName, attributeValue + SEGMENTATION_ID_SEPARATOR + i));

                BaseSequence[] partitions = segmentationCoder.encode(byteCoded);
                List<EncodedLine> pairs = new ArrayList<>(attributes.length);
                for (int i = 0; i < partitions.length; i++) {
                    pairs.add(new EncodedLine(
                            attributes[i],
                            partitions[i]
                    ));
                }
                return pairs;
            }));
            service.shutdown();
        });

        singleExecutor.shutdown();
        return service.stream();
    }

    protected Stream<DecodedLine<?>> decode(Stream<List<EncodedLine>> encodedLines, boolean parallel) {
        PooledCompletionService<DecodedLine<?>> service = new PooledCompletionService<>(FuncUtils.pool(parallel));
        ExecutorService singleExecutor = Executors.newSingleThreadExecutor();
        singleExecutor.execute(() -> {
            encodedLines.forEach(p -> service.submit(() -> {
                Attribute<?> attribute = attributeCoder.decode(p.get(0).getT1());
                return new DecodedLine<>(
                        attribute,
                        rangeCompressor.decode(new String(byteCoder.decode(segmentationCoder.decode(p.stream().map(Pair::getT2).toArray(BaseSequence[]::new))))));
            }));
            service.shutdown();
        });

        singleExecutor.shutdown();
        return service.stream();
    }

    /**
     * Encodes the given csv lines in sequential mode. Due to segmentation, each "line" is encoded to a list of EncodedLine.
     * @param lines the input vsc lines to be encoded.
     * @return a stream of List < EncodedLine >.
     */
    @Override
    public Stream<List<EncodedLine>> encode(Stream<CsvLine> lines) {
        return encode(lines, Collectors.groupingBy(line -> line.get(attributeName), Collectors.mapping(line -> line.get(keyName), new DeltaCode.RangeCollector(rangeCompressor, true))), false);
    }

    /**
     * Encodes the given csv lines in parallel mode. Due to segmentation, each "line" is encoded to a list of EncodedLine.
     * @param lines the input vsc lines to be encoded.
     * @return a stream of List < EncodedLine >.
     */
    @Override
    public Stream<List<EncodedLine>> encodeParallel(Stream<CsvLine> lines) {
        return encode(lines.parallel(), Collectors.groupingByConcurrent(line -> line.get(attributeName), Collectors.mapping(line -> line.get(keyName), new DeltaCode.RangeCollector(rangeCompressor, false))), true);
    }

    /**
     * Decodes the given Encoded lines' lists in sequential mode. This method requires that each list contains the corresponding segments of an encoded csv line in order.
     * @param encodedLines the encoded stream.
     * @return the decoded stream of DecodedLine < ? >
     */
    @Override
    public Stream<DecodedLine<?>> decode(Stream<List<EncodedLine>> encodedLines) {
        return decode(encodedLines, false);
    }

    /**
     * Decodes the given Encoded lines' lists in parallel mode. This method requires that each list contains the corresponding segments of an encoded csv line in order.
     * @param encodedLines the encoded lines' stream.
     * @return the decoded stream of DecodedLine < ? >
     */
    @Override
    public Stream<DecodedLine<?>> decodeParallel(Stream<List<EncodedLine>> encodedLines) {
        return decode(encodedLines, true);
    }

    /**
     * Decodes the given Encoded lines' lists in sequential mode. This method does not require any ordering of the segments. This method is useful to parse and decode a large number of EncodedLine that need to be grouped and sorted.
     * @param encodedLines the encoded lines' stream.
     * @return the decoded stream of DecodedLine < ? >
     */
    @Override
    public Stream<DecodedLine<?>> decodeFromUnordered(Stream<EncodedLine> encodedLines) {
        return encodedLines
                .map(line -> new Pair<>(attributeCoder.decode(line.getT1()), line.getT2()))
                .collect(Collectors.groupingBy(p -> {
                    String key = p.getT1().getName();
                    String value = p.getT1().getValue().toString();
                    int index = value.lastIndexOf(AttributeCoder.SEGMENTATION_ID_SEPARATOR);
                    return index >= 0 ? new Attribute<>(key, value.substring(0, index)) : new Attribute<>(key, value);
                }, Collectors.mapping(p -> p,
                        Collectors.toCollection(() -> new TreeSet<>(Comparator.comparing(p -> {
                            String value = p.getT1().getValue().toString();
                            int index = value.lastIndexOf(AttributeCoder.SEGMENTATION_ID_SEPARATOR);
                            return index >= 0? Integer.parseInt(value.substring(index + 1)) : Integer.MIN_VALUE;
                        }))))))
                .entrySet().stream()
                .map(e -> new DecodedLine<>(e.getKey(), rangeCompressor.decode(new String(byteCoder.decode(segmentationCoder.decode(e.getValue().stream().map(Pair::getT2).toArray(BaseSequence[]::new)))))));
    }

    /**
     * Decodes the given Encoded lines' lists in parallel mode. This method does not require any ordering of the segments. This method is useful to parse and decode a large number of EncodedLine that need to be grouped and sorted.
     * @param encodedLines the encoded lines' stream.
     * @return the decoded stream of DecodedLine < ? >
     */
    @Override
    public Stream<DecodedLine<?>> decodeFromUnorderedParallel(Stream<EncodedLine> encodedLines) {
        return encodedLines.parallel()
                .map(line -> new Pair<>(attributeCoder.decode(line.getT1()), line.getT2()))
                .collect(Collectors.groupingByConcurrent(p -> {
                    String key = p.getT1().getName();
                    String value = p.getT1().getValue().toString();
                    int index = value.lastIndexOf(AttributeCoder.SEGMENTATION_ID_SEPARATOR);
                    return index >= 0 ? new Attribute<>(key, value.substring(0, index)) : new Attribute<>(key, value);
                }, Collectors.mapping(p -> p,
                        Collectors.toCollection(() -> new TreeSet<>(Comparator.comparing(p -> {
                            String value = p.getT1().getValue().toString();
                            int index = value.lastIndexOf(AttributeCoder.SEGMENTATION_ID_SEPARATOR);
                            return index >= 0? Integer.parseInt(value.substring(index + 1)) : Integer.MIN_VALUE;
                        }))))))
                .entrySet().stream().parallel()
                .map(e -> new DecodedLine<>(e.getKey(), rangeCompressor.decode(new String(byteCoder.decode(segmentationCoder.decode(e.getValue().stream().map(Pair::getT2).toArray(BaseSequence[]::new)))))));
    }
}
