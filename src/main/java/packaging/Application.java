package packaging;

import core.Attribute;
import core.BaseSequence;
import core.dnarules.BasicDNARules;
import core.dnarules.DNARule;
import core.dnarules.SuperBasicDNARules;
import dnacoders.*;
import dnacoders.attributecoders.AttributeCoder;
import dnacoders.attributecoders.NonUniqueAttributeCoder;
import dnacoders.attributecoders.UniqueAttributeCoder;
import dnacoders.dnaconvertors.Bin;
import dnacoders.dnaconvertors.NaiveQuattro;
import dnacoders.dnaconvertors.RotatingQuattro;
import dnacoders.dnaconvertors.RotatingTre;
import dnacoders.headercoders.BasicDNAPadder;
import dnacoders.headercoders.PermutationCoder;
import utils.DNAPacker;
import utils.FuncUtils;
import utils.Pair;
import utils.compression.DeltaCode;
import utils.compression.GZIP;
import utils.csv.BufferedCsvReader;
import utils.csv.BufferedCsvWriter;
import utils.csv.CsvLine;
import utils.fasta.ReadableFASTAFile;
import utils.fasta.WriteableFASTAFile;
import utils.lsh.LSH;
import utils.rq.RQCoder;
import java.io.File;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class Application {

    public static void main(String... args) {
        ConfigFile config = new ConfigFile(FuncUtils.tryOrElse(() -> args[0], () -> "params.ini"));
        boolean force = FuncUtils.tryOrElse(() -> Boolean.valueOf(args[1]), () -> false);

        System.out.println("Parsed the following parameters:\n-----------------------------------------------------------------\n" + config);
        System.out.println();
        if (!force && !approveParameters()) {
            System.out.println("Parameters were not approved. Exiting!");
            return;
        }
        ConfigFile.APP_MODE mode = config.getMode();

        if (mode.doEncode()) {
            Instant startEncoding = Instant.now();
            encode(config);
            System.out.println("\n-----------------------------------------------------------------\nEncoding took: " + elapsedTime(Duration.between(startEncoding, Instant.now())) + "\n-----------------------------------------------------------------\n");
        }

        if (mode.doDecode()) {
            Instant startDecoding = Instant.now();
            decode(config);
            System.out.println("\n-----------------------------------------------------------------\nDecoding took: " + elapsedTime(Duration.between(startDecoding, Instant.now())) + "\n-----------------------------------------------------------------\n");
        }
    }

    public static String elapsedTime(Duration duration) {
        long millis = duration.toMillis();
        long days = TimeUnit.MILLISECONDS.toDays(millis);
        millis = Math.max(0, millis - TimeUnit.DAYS.toMillis(days));


        long hours = TimeUnit.MILLISECONDS.toHours(millis);
        millis = Math.max(0, millis - TimeUnit.HOURS.toMillis(hours));

        long minutes = TimeUnit.MILLISECONDS.toMinutes(millis);
        millis = Math.max(0, millis - TimeUnit.MINUTES.toMillis(minutes));

        long seconds = TimeUnit.MILLISECONDS.toSeconds(millis);
        millis = Math.max(0, millis - TimeUnit.SECONDS.toMillis(seconds));

        return days + " days, " + hours + " hours, " + minutes + " minutes, " + seconds + " seconds, " + millis + " millis";
    }

    public static void encode(ConfigFile config) {
        float rqMaxError = config.getRqMaxError();
        boolean parallel = config.getParallel();
        String tablePath = config.getTablePath();
        String encodePath = config.getEncodePath();

        DNARule basicRules = BasicDNARules.INSTANCE;
        DNARule superBasicRules = SuperBasicDNARules.INSTANCE;

        LSH lsh = new LSH(config.getLshK(), config.getLshR(), config.getLshB());
        Coder<BaseSequence, BaseSequence> distanceCoder = getDistanceCoder(config, basicRules, lsh);

        DNACoder<Attribute<?>> attributeCoder = attributeEncoder(
                false,
                        config.getCBBsMinimumLength(),
                        config.getCBBsPermutations(),
                        extractDnaConvertor(config.getCBBsDnaConvertor()),
                        basicRules,
                        lsh
        );

        SegmentationCoder segmentationCoder = new MySegmentationCoder(config.getSegmentationLength() - DNAPacker.pack(config.getSegmentationPermutations() - 1).length(), distanceCoder, config.getSegmentationGcCorrections());
        SegmentationCoder doNothingSegmentationCoder = new DoNothingSegmentation();


        Supplier<BufferedCsvReader> sourceSupplier = () -> new BufferedCsvReader(tablePath);
        FuncUtils.safeCall(() -> new File(encodePath).mkdirs());
        System.out.println("[encoding unique attributes]");
        int counter = 0;
        int seqsCounter;
        for (ConfigFile.UniqueAttribute att : config.getUniqueAttributes()) {
            seqsCounter = 0;
            WriteableFASTAFile writer = new WriteableFASTAFile(encodePath + "/u_mapping_" + (counter++) + "_" + att.getKey() + ".fa", false);
            UniqueAttributeCoder coder = new UniqueAttributeCoder(
                    attributeCoder,
                    rq(att.getGZIP(), rqMaxError, superBasicRules),
                    att.getSegmentation() ? segmentationCoder : doNothingSegmentationCoder,
                    att.getKey(),
                    att.getMappingAttributes(),
                    att.getMappingAttributesTypes()
            );

            System.out.println("-> encoding: " + att);
            var it = encode(coder, sourceSupplier.get(), parallel).iterator();
            while(it.hasNext()) {
                for (AttributeCoder.EncodedLine encodedLine : it.next()) {
                    seqsCounter++;
                    writer.append(new BaseSequence(encodedLine.getT1(), encodedLine.getT2()), seqsCounter + "-" + encodedLine.getT1().length());
                }
            }
            writer.close();
        }

        System.out.println("[encoding non-unique attributes]");
        counter = 0;
        for (ConfigFile.NonUniqueAttribute att : config.getNonUniqueAttributes()) {
            seqsCounter = 0;
            WriteableFASTAFile writer = new WriteableFASTAFile(encodePath + "/mapping_" + (counter++) + "_" + att.getKey() + ".fa", false);
            NonUniqueAttributeCoder coder = new NonUniqueAttributeCoder(
                    att.getKey(),
                    att.getMappingAttribute(),
                    attributeCoder,
                    rq(att.getGZIP(), rqMaxError, superBasicRules),
                    DeltaCode.DEFAULT_RANGE_DELTA_COMPRESSOR,
                    att.getSegmentation() ? segmentationCoder : doNothingSegmentationCoder
            );
            System.out.println("-> encoding: " + att);
            var it = encode(coder, sourceSupplier.get(), parallel).iterator();
            while(it.hasNext()) {
                for (AttributeCoder.EncodedLine encodedLine : it.next()) {
                    seqsCounter++;
                    writer.append(new BaseSequence(encodedLine.getT1(), encodedLine.getT2()), seqsCounter + "-" + encodedLine.getT1().length());
                }
            }
            writer.close();
        }
    }

    private static Coder<BaseSequence, BaseSequence> getDistanceCoder(ConfigFile config, DNARule basicRules, LSH lsh) {
        if (config.getSegmentationPermutations() <= 0)
            return new Coder<>() {
                @Override
                public BaseSequence encode(BaseSequence seq) {
                    return seq;
                }
                @Override
                public BaseSequence decode(BaseSequence seq) {
                    return seq;
                }
            };

        return new DistanceCoder(
                config.getSegmentationPermutations(),
                lsh,
                basicRules,
                config.getDistanceCoderCError(),
                config.getDistanceCoderCDistance()
        );
    }


    public static void decode(ConfigFile config) {
        DNACoder<Attribute<?>> attributeDecoder = attributeDecoder(
                config.getParallel(),
                0,
                config.getCBBsPermutations(),
                extractDnaConvertor(config.getCBBsDnaConvertor())
        );

        Coder<BaseSequence, BaseSequence> distanceCoder = getDistanceCoder(config, null, null);

        SegmentationCoder segmentationCoder = new MySegmentationCoder(100, distanceCoder, 0);
        SegmentationCoder doNothingSegmentationCoder = new DoNothingSegmentation();
        String decodePath = config.getDecodePath();

        int counter = 0;
        FuncUtils.safeCall(() -> new File(decodePath).mkdirs());
        System.out.println("[decoding unique attributes]");
        for (ConfigFile.UniqueAttribute att : config.getUniqueAttributes()) {
            System.out.println("-> decoding: " + att);
            BufferedCsvWriter writer = new BufferedCsvWriter(decodePath + "/u_mapping_" + counter + "_" + att.getKey() + "_decoded.txt", false);
            UniqueAttributeCoder coder = new UniqueAttributeCoder(
                    attributeDecoder,
                    rq(att.getGZIP(), 1f, seq -> 0f),
                    att.getSegmentation() ? segmentationCoder : doNothingSegmentationCoder,
                    att.getKey(),
                    att.getMappingAttributes(),
                    att.getMappingAttributesTypes()
            );

            decode(coder, new ReadableFASTAFile(config.getEncodePath() + "/u_mapping_" + (counter++) + "_" + att.getKey() + ".fa"), config.getParallel()).map(Pair::toString).forEach(writer::appendNewLine);
            writer.close();
        }

        counter = 0;
        System.out.println("decoding non-unique attributes");
        for (ConfigFile.NonUniqueAttribute att : config.getNonUniqueAttributes()) {
            System.out.println("-> decoding: " + att);
            BufferedCsvWriter writer = new BufferedCsvWriter(config.getDecodePath() + "/mapping_" + counter + "_" + att.getKey() + "_decoded.txt", false);
            NonUniqueAttributeCoder coder = new NonUniqueAttributeCoder(
                    att.getKey(),
                    att.getMappingAttribute(),
                    attributeDecoder,
                    rq(att.getGZIP(), 1f, seq -> 0f),
                    DeltaCode.DEFAULT_RANGE_DELTA_COMPRESSOR,
                    att.getSegmentation() ? segmentationCoder : doNothingSegmentationCoder
            );

            decode(coder, new ReadableFASTAFile(config.getEncodePath() + "/mapping_" + (counter++) + "_" + att.getKey() + ".fa"), config.getParallel()).map(Pair::toString).forEach(writer::appendNewLine);
            writer.close();
        }
    }

    static DNACoder<String> extractDnaConvertor(String dnaConvertorName) {
        if (dnaConvertorName.equalsIgnoreCase(RotatingQuattro.class.getSimpleName()))
            return RotatingQuattro.INSTANCE;
        if (dnaConvertorName.equalsIgnoreCase(RotatingTre.class.getSimpleName()))
            return RotatingTre.INSTANCE;
        if (dnaConvertorName.equalsIgnoreCase(Bin.class.getSimpleName()))
            return Bin.INSTANCE;
        if (dnaConvertorName.equalsIgnoreCase(NaiveQuattro.class.getSimpleName()))
            return NaiveQuattro.INSTANCE;
        else
            throw new IllegalArgumentException("Cannot find DNA convertor: " + dnaConvertorName);
    }


    static DNACoder<byte[]> rq(boolean useGZIP, float rqMaxError, DNARule rules) {
        var rq = new RQCoder(
                seq -> rules.evalErrorProbability(seq) <= rqMaxError,
                seq -> rules.evalErrorProbability(seq) <= rqMaxError
        );

        return useGZIP ? DNACoder.fuse(GZIP.INSTANCE, rq) : rq;
    }

    static Stream<List<AttributeCoder.EncodedLine>> encode(AttributeCoder<CsvLine, AttributeCoder.DecodedLine<?>, List<AttributeCoder.EncodedLine>> coder, BufferedCsvReader reader, boolean parallel) {
        return parallel ? coder.encodeParallel(reader.stream()) : coder.encode(reader.stream());
    }

    static Stream<AttributeCoder.DecodedLine<?>> decode(AttributeCoder<CsvLine, AttributeCoder.DecodedLine<?>, List<AttributeCoder.EncodedLine>> coder, ReadableFASTAFile reader, boolean parallel) {
        Stream<AttributeCoder.EncodedLine> encodedLines = reader.stream().map(e -> {
            int spLength = Integer.parseInt(e.getCaption().split("-")[1]);
            return new AttributeCoder.EncodedLine(e.getSeq().window(0, spLength), e.getSeq().window(spLength));
        });

        return parallel ? coder.decodeFromUnorderedParallel(encodedLines) : coder.decodeFromUnordered(encodedLines);
    }

    static boolean approveParameters() {
        System.out.println("-> Note: Any existing file with the same name will be overridden");
        System.out.println("Are these parameters correct? [y/n]");
        String answer = FuncUtils.superSafeCall(() -> new Scanner(System.in).nextLine());
        return (answer != null) && (answer.equalsIgnoreCase("y") || answer.equalsIgnoreCase("true") || answer.equals("1") || answer.equalsIgnoreCase("yes"));
    }



    public static DNACoder<Attribute<?>> attributeEncoder(boolean parallel, int targetLength, int permsCount, DNACoder<String> dnaConvertor, DNARule rules, LSH lsh) {
        DNACoder<Attribute<?>> coder =  DNACoder.fuse(
                AttributeMapper.newInstance(dnaConvertor),
                new BasicDNAPadder(targetLength - DNAPacker.pack(permsCount - 1).length())
        );
        DNACoder<BaseSequence> lshUpdater = new DNACoder<>() {
            @Override
            public BaseSequence encode(BaseSequence seq) {
                lsh.insertSafe(seq);
                return seq;
            }
            @Override
            public BaseSequence decode(BaseSequence seq) {
                return seq;
            }
        };

        if (permsCount == 0)
            return DNACoder.fuse(coder, lshUpdater);

        return DNACoder.fuse(
                coder,
                new PermutationCoder(parallel, permsCount, seq -> -rules.evalErrorProbability(seq)),
                lshUpdater
        );
    }

    public static DNACoder<Attribute<?>> attributeDecoder(boolean parallel, int targetLength, int permsCount, DNACoder<String> dnaConvertor) {
        DNACoder<Attribute<?>> coder =  DNACoder.fuse(
                AttributeMapper.newInstance(dnaConvertor),
                new BasicDNAPadder(targetLength - DNAPacker.pack(permsCount - 1).length())
        );

        if (permsCount == 0)
            return coder;

        return DNACoder.fuse(
                coder,
                new PermutationCoder(parallel, permsCount, seq -> 0f)
        );
    }

    private static class MySegmentationCoder extends SegmentationCoder {
        Coder<BaseSequence, BaseSequence> distCoder;
        public MySegmentationCoder(int targetSplitLength, Coder<BaseSequence, BaseSequence> distCoder, int gcCorrections) {
            super(targetSplitLength, gcCorrections);
            this.distCoder = distCoder;
        }

        @Override
        public BaseSequence[] encode(BaseSequence seq) {
            BaseSequence[] segments = super.encode(seq);
            for (int i = 0; i < segments.length; i++)
                segments[i] = distCoder.encode(segments[i]);

            return segments;
        }

        @Override
        public BaseSequence decode(BaseSequence[] orderedPartitions) {
            for (int i = 0; i < orderedPartitions.length; i++)
                orderedPartitions[i] = distCoder.decode(orderedPartitions[i]);

            return super.decode(orderedPartitions);
        }
    }

    private static class DoNothingSegmentation extends SegmentationCoder {
        public DoNothingSegmentation(int targetLength, int numGcCorrection) {
            super(targetLength, numGcCorrection);
        }

        public DoNothingSegmentation() {
            super(100, 0);
        }

        @Override
        public BaseSequence[] encode(BaseSequence seq) {
            return new BaseSequence[] {seq};
        }

        @Override
        public BaseSequence decode(BaseSequence[] seqs) {
            return seqs[0];
        }

        @Override
        public int numSegments(int len) {
            return 1;
        }
    }
}
