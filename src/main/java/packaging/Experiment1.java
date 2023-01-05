package packaging;

import core.Attribute;
import core.BaseSequence;
import core.dnarules.BasicDNARules;
import dnacoders.AttributeMapper;
import dnacoders.DistanceCoder;
import dnacoders.dnaconvertors.NaiveQuattro;
import dnacoders.dnaconvertors.Bin;
import dnacoders.dnaconvertors.RotatingQuattro;
import dnacoders.dnaconvertors.RotatingTre;
import dnacoders.headercoders.BasicDNAPadder;
import dnacoders.headercoders.PermutationCoder;
import utils.analyzing.Aggregator;
import utils.csv.BufferedCsvWriter;
import utils.lsh.LSH;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class Experiment1 {

    public static void main(String[] args) {
        Instant start = Instant.now();
        Attribute<Integer> a = new Attribute<>("Id", 0);
        var targetLengths = new int[] {0, 20, 40, 60, 80};
        var numsPermutations = new int[] {0, 2, 4, 8, 16, 32, 64};
        var dnaConvertors = List.of(RotatingQuattro.INSTANCE, RotatingTre.INSTANCE, NaiveQuattro.INSTANCE, Bin.INSTANCE);
        int size = 1_000_000;
        var rules = BasicDNARules.INSTANCE;
        BufferedCsvWriter csv = new BufferedCsvWriter("experiment_1.csv", false);
        if (csv.isEmpty()) {
            csv.appendNewLine(
                    "DNA Convertor",
                    "Permutations",
                    "#Sequences",
                    "L_{CBB}",
                    "min(error)",
                    "max(error)",
                    "avg(error)",
                    "var(error)",
                    "std_dev(error)",
                    "min(dist)",
                    "max(dist)",
                    "avg(dist)",
                    "var(dist)",
                    "std_dev(dist)"
            );
        }

        for (var dnaConvertor : dnaConvertors) {
            var rotator = AttributeMapper.newInstance(dnaConvertor);
            List<BaseSequence> rotatedSeqs = IntStream.range(0, size).parallel().mapToObj(id -> rotator.encode(a.newValue(id))).toList();
            for (int targetLength : targetLengths) {
                var padder = new BasicDNAPadder(targetLength);
                System.out.printf("started generating %d sequences (DNA Convertor: %s, targetLength: %d)%n", size, dnaConvertor.getClass().getSimpleName(), targetLength);
                List<BaseSequence> paddedSeqs = rotatedSeqs.stream().parallel().map(padder::encode).toList();
                System.out.println("..sequences are generated");
                for (int numPermutations : numsPermutations) {
                    List<BaseSequence> permutedSeqs;
                    System.out.printf("..computing (DNA Convertor: %s, L_{CBB}: %d, numPermutations: %d)%n%n", dnaConvertor.getClass().getSimpleName(), targetLength, numPermutations);
                    if (numPermutations > 0) {
                        PermutationCoder pc = new PermutationCoder(false, numPermutations, seq -> -rules.evalErrorProbability(seq));
                        permutedSeqs = paddedSeqs.stream().parallel().map(pc::encode).toList();
                    }
                    else {
                        permutedSeqs = paddedSeqs;
                    }

                    System.out.println("..final sequences generated");
                    System.out.println("..computing error aggregate");
                    var errAgg = Aggregator.aggregateNumbers(permutedSeqs.stream().parallel().mapToDouble(rules::evalErrorProbability).toArray(), true);
                    System.out.println("..inserting into LSH to compute distance aggregate");
                    var lsh = new LSH(4, 200, 20);
                    lsh.insertParallel(permutedSeqs);
                    System.out.printf("..%d sequences were inserted into the LSH --> computing the distance aggregate%n", size);
                    var distAgg = distStats(permutedSeqs, lsh);
                    System.out.println("..flushing results into csv file");
                    csv.appendNewLine(
                            Stream.of(
                                    dnaConvertor.getClass().getSimpleName(),
                                    numPermutations,
                                    size,
                                    targetLength,
                                    errAgg.min(),
                                    errAgg.max(),
                                    errAgg.avg(),
                                    errAgg.var(),
                                    errAgg.stdDev(),
                                    distAgg.min(),
                                    distAgg.max(),
                                    distAgg.avg(),
                                    distAgg.var(),
                                    distAgg.stdDev()
                            ).map(Object::toString).toArray(String[]::new));

                    csv.flush();
                    System.out.printf("finished (DNA Convertor: %s, L_{CBB}: %d, numPermutations: %d)%n%n", dnaConvertor.getClass().getSimpleName(), targetLength, numPermutations);
                }
            }
        }
        csv.close();

        System.out.println("\n-----------------------------------------------------------\nExperiment 1 took: " + Application.elapsedTime(Duration.between(start, Instant.now())) + "\n-----------------------------------------------------------\n");
    }


    public static Aggregator.NumberAggregates distStats(List<BaseSequence> seqs, LSH lsh) {
        return Aggregator.aggregateNumbers(seqs.stream().parallel().mapToDouble(seq -> Math.min(DistanceCoder.distanceScoreExclusive(seq, lsh, false), DistanceCoder.distanceScore(seq.complement(), lsh, false))).toArray(), true);
    }
}

