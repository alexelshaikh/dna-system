package dnacoders.attributecoders;

import core.BaseSequence;
import core.Attribute;
import dnacoders.Coder;
import dnacoders.DNACoder;
import dnacoders.SegmentationCoder;
import utils.*;
import utils.csv.CsvLine;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class UniqueAttributeCoder implements AttributeCoder<CsvLine, AttributeCoder.DecodedLine<?>, List<AttributeCoder.EncodedLine>> {

    private final DNACoder<Attribute<?>> attributeCoder;
    private final DNACoder<byte[]> byteCoder;
    private final String key;
    private final String[] values;
    private final Packer.Type[] types;
    private final SegmentationCoder segmentationCoder;
    private final Coder<CsvLine, byte[]> csvLinePacker;

    /**
     * Creates an encoder for a unique attribute.
     * @param attributeCoder the instance to encode attributes.
     * @param byteCoder the instance to encode a byte array (e.g. RQCoder).
     * @param segmentationCoder the instance to apply segmentation.
     * @param key the unique attribute's name to be encoded.
     * @param values the other attributes' names this key will map to.
     * @param types the other attributes' types.
     */
    public UniqueAttributeCoder(DNACoder<Attribute<?>> attributeCoder, DNACoder<byte[]> byteCoder, SegmentationCoder segmentationCoder, String key, String[] values, Packer.Type[] types) {
        if (values.length != types.length)
            throw new RuntimeException("values.length != types.length");
        this.attributeCoder = attributeCoder;
        this.byteCoder = byteCoder;
        this.key = key;
        this.values = values;
        this.types = types;
        this.segmentationCoder = segmentationCoder;
        this.csvLinePacker = CSVLinePacker.fromTypeByAttributeNameArrays(values, types);
    }

    /**
     * Encodes the given csv lines in sequential mode. Due to segmentation, each "line" is encoded to a list of EncodedLine.
     * @param lines the input vsc lines to be encoded.
     * @return a stream of List < EncodedLine >.
     */
    @Override
    public Stream<List<EncodedLine>> encode(Stream<CsvLine> lines) {
        PooledCompletionService<List<EncodedLine>> service = new PooledCompletionService<>(FuncUtils.pool(lines.isParallel()));
        ExecutorService singleExecutor = Executors.newSingleThreadExecutor();
        singleExecutor.execute(() -> {
            Attribute<String> keyAttribute = new Attribute<>(key);
            lines.forEach(line -> service.submit(() -> {
                byte[] bytes = csvLinePacker.encode(line);
                BaseSequence byteCoded = byteCoder.encode(bytes);
                String attributeValue = line.get(key);
                int numPartitions = segmentationCoder.numSegments(byteCoded.length());
                BaseSequence[] attributes = new BaseSequence[numPartitions + 1];
                attributes[0] = attributeCoder.encode(keyAttribute.newValue(attributeValue));
                for (int i = 1; i < attributes.length; i++)
                    attributes[i] = attributeCoder.encode(keyAttribute.newValue(attributeValue + SEGMENTATION_ID_SEPARATOR + i));

                BaseSequence[] partitions = segmentationCoder.encode(byteCoded);
                List<EncodedLine> encodedLines = new ArrayList<>(attributes.length);
                for (int i = 0; i < partitions.length; i++) {
                    encodedLines.add(new EncodedLine(
                            attributes[i],
                            partitions[i]
                    ));
                }
                return encodedLines;
            }));
            service.shutdown();
        });

        singleExecutor.shutdown();
        return service.stream();
    }

    /**
     * Decodes the given Encoded lines' lists in sequential mode. This method requires that each list contains the corresponding segments of an encoded csv line in order.
     * @param encodedLines the encoded stream.
     * @return the decoded stream of DecodedLine < ? >
     */
    @Override
    public Stream<DecodedLine<?>> decode(Stream<List<EncodedLine>> encodedLines) {
        PooledCompletionService<DecodedLine<?>> service = new PooledCompletionService<>(FuncUtils.pool(encodedLines.isParallel()));
        ExecutorService singleExecutor = Executors.newSingleThreadExecutor();
        singleExecutor.execute(() -> {
            encodedLines.forEach(pairs -> service.submit(() -> {
                Attribute<?> attribute = attributeCoder.decode(pairs.get(0).getT1());
                return decodeEntry(attribute, pairs);
            }));
            service.shutdown();
        });

        singleExecutor.shutdown();
        return service.stream();
    }

    /**
     * Encodes the given csv lines in parallel mode. Due to segmentation, each "line" is encoded to a list of EncodedLine.
     * @param lines the input vsc lines to be encoded.
     * @return a stream of List < EncodedLine >.
     */
    @Override
    public Stream<List<EncodedLine>> encodeParallel(Stream<CsvLine> lines) {
        return encode(lines.parallel());
    }

    /**
     * Decodes the given Encoded lines' lists in parallel mode. This method requires that each list contains the corresponding segments of an encoded csv line in order.
     * @param encodedLines the encoded lines' stream.
     * @return the decoded stream of DecodedLine < ? >
     */
    @Override
    public Stream<DecodedLine<?>> decodeParallel(Stream<List<EncodedLine>> encodedLines) {
        return decode(encodedLines.parallel());
    }

    private DecodedLine<?> decodeEntry(Attribute<?> att, Collection<? extends Pair<?, BaseSequence>> pairs) {
        CsvLine decoded = csvLinePacker.decode(byteCoder.decode(segmentationCoder.decode(pairs.stream().map(Pair::getT2).toArray(BaseSequence[]::new))));
        return new DecodedLine<>(
                att,
                decoded
        );
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
                .map(e -> decodeEntry(e.getKey(), e.getValue()));
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
                .map(e -> decodeEntry(e.getKey(), e.getValue()));
    }
}