package packaging;

import core.BaseSequence;
import core.dnarules.BasicDNARules;
import core.dnarules.DNARule;
import dnacoders.DistanceCoder;
import utils.Streamable;
import utils.analyzing.Aggregator;
import utils.csv.BufferedCsvWriter;
import utils.fasta.ReadableFASTAFile;
import utils.lsh.LSH;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

public class Experiment2 {
    public static void main(String[] args) {
        Instant start = Instant.now();
        System.out.println("----------------- starting experiment2");

        Instant applicationSegStart = Instant.now();
        System.out.println("----------------- running Application on params_seg.ini");
        Application.main("params_seg.ini", "true");
        Instant applicationNoSegStart = Instant.now();
        System.out.println("----------------- finished Application on params_seg.ini in: " + Application.elapsedTime(Duration.between(applicationSegStart, applicationNoSegStart)));

        System.out.println("----------------- running Application on params_noseg.ini");
        Application.main("params_noseg.ini", "true");
        System.out.println("----------------- finished Application on params_noseg.ini in: " + Application.elapsedTime(Duration.between(applicationNoSegStart, Instant.now())));

        System.out.println("----------------- evaluating for the segmentation case");
        Instant evalSegStart = Instant.now();


        eval(true,
                new LSH(4, 200, 20),
                new LSH(6, 200, 20),
                new LSH(6, 200, 20),
                "encoded/u_mapping_0_Id.fa", "encoded/mapping_0_icao24.fa");

        Instant evalNoSegStart = Instant.now();
        System.out.println("----------------- the evaluation of the sequences with segmentation took: " + Application.elapsedTime(Duration.between(evalSegStart, evalNoSegStart)));
        System.out.println("----------------- evaluating for the no-segmentation case");

        eval(false,
                new LSH(4, 200, 20),
                new LSH(6, 200, 20),
                new LSH(6, 200, 20),
                "encoded_noseg/u_mapping_0_Id.fa", "encoded_noseg/mapping_0_icao24.fa");

        Instant end = Instant.now();
        System.out.println("----------------- the evaluation of the sequences without segmentation took: " + Application.elapsedTime(Duration.between(evalNoSegStart, end)));
        System.out.println("----------------- the whole evaluation took: " + Application.elapsedTime(Duration.between(evalSegStart, end)));
        System.out.println("----------------- everything took: " + Application.elapsedTime(Duration.between(start, end)));
        System.out.println("----------------- done");
    }

    static Aggregator.NumberAggregates distAggregate(List<BaseSequence> seqs, LSH lsh) {
        System.out.println("..inserting " + seqs.size() + " sequences into LSH");
        lsh.insertParallel(seqs);
        System.out.println("..calculating distance aggregate");
        return Aggregator.aggregateNumbers(seqs.stream().parallel()
                .mapToDouble(seq -> Math.min(DistanceCoder.distanceScoreExclusive(seq, lsh, false), DistanceCoder.distanceScore(seq.complement(), lsh, false))).toArray(), true);
    }

    static Aggregator.NumberAggregates errorAggregate(List<BaseSequence> seqs, DNARule rules) {
        System.out.println("..calculating error score");
        return Aggregator.aggregateNumbers(seqs.stream().parallel()
                .mapToDouble(rules::evalErrorProbability).toArray(), true);
    }

    static Aggregator.NumberAggregates lengthAggregate(List<BaseSequence> seqs) {
        System.out.println("..calculating length score");
        return Aggregator.aggregateNumbers(seqs.stream().parallel()
                .mapToDouble(BaseSequence::length).toArray(), true);
    }

    static Aggregator.NumberAggregates[] aggregate(List<BaseSequence> seqs, LSH lsh, DNARule rule) {
        return new Aggregator.NumberAggregates[] {
                distAggregate(seqs, lsh),
                errorAggregate(seqs, rule),
                lengthAggregate(seqs)
        };
    }

    static void eval(boolean segmentation, LSH cbbsLSH, LSH infoDnaLSH, LSH oligosLSH, String... filePaths) {
        System.out.println("evaluating " + Arrays.toString(filePaths));
        List<ReadableFASTAFile> fastaFiles = Arrays.stream(filePaths).map(ReadableFASTAFile::new).toList();
        BufferedCsvWriter writer = new BufferedCsvWriter("experiment2.csv", true);
        if (writer.isEmpty()) {
            writer.appendNewLine(
                    "type",
                    "count",
                    "min(dist)",
                    "max(dist)",
                    "avg(dist)",
                    "std_dev(dist)",
                    "min(error)",
                    "max(error)",
                    "avg(error)",
                    "std_dev(error)",
                    "min(length)",
                    "max(length)",
                    "avg(length)",
                    "std_dev(length)",
                    "lsh_k",
                    "lsh_r",
                    "lsh_b",
                    "segmentation"
            );
        }

        DNARule rules = BasicDNARules.INSTANCE;
        List<BaseSequence> cbbs = new ArrayList<>();
        List<BaseSequence> infoDNAs = new ArrayList<>();
        List<BaseSequence> oligos = new ArrayList<>();
        System.out.println("reading sequences...");
        fastaFiles.stream().flatMap(Streamable::stream).forEach(fa -> {
            String caption = fa.getCaption();
            int cbbLength = Integer.parseInt(caption.substring(caption.indexOf('-') + 1));
            cbbs.add(fa.getSeq().window(0, cbbLength));
            infoDNAs.add(fa.getSeq().window(cbbLength));
            oligos.add(fa.getSeq());
        });
        fastaFiles.forEach(ReadableFASTAFile::close);

        int count = cbbs.size();
        System.out.println("read " + count + " sequences");

        System.out.println("\ncbbs:");
        var cbbsAggs = aggregate(cbbs, cbbsLSH, rules);
        int cbbsK = cbbsLSH.getK();
        int cbbsR = cbbsLSH.getR();
        int cbbsB = cbbsLSH.getB();
        System.out.println("------------------------------\n");

        System.out.println("info-DNA:");
        var infoDnaAggs = aggregate(infoDNAs, infoDnaLSH, rules);
        int infoDnaK = infoDnaLSH.getK();
        int infoDnaR = infoDnaLSH.getR();
        int infoDnaB = infoDnaLSH.getB();
        System.out.println("------------------------------\n");

        System.out.println("oligos:");
        var oligosAggs = aggregate(oligos, oligosLSH, rules);
        int oligosK = oligosLSH.getK();
        int oligosR = oligosLSH.getR();
        int oligosB = oligosLSH.getB();

        writer.appendNewLine(csvLine(segmentation, "CBBs", cbbs.size(), cbbsAggs, cbbsK, cbbsR, cbbsB));
        writer.appendNewLine(csvLine(segmentation, "Info-DNA", infoDNAs.size(), infoDnaAggs, infoDnaK, infoDnaR, infoDnaB));
        writer.appendNewLine(csvLine(segmentation, "Oligos", oligos.size(), oligosAggs, oligosK, oligosR, oligosB));

        writer.close();

        System.out.println("------------------------------\n");
    }

    static String[] csvLine (boolean segmentation, String type, int count, Aggregator.NumberAggregates[] aggs, int k, int r, int b) {
        return Stream.of(type,
                        count,
                        Arrays.stream(aggs).flatMap(agg -> Stream.of(agg.min(), agg.max(), agg.avg(), agg.stdDev())),
                        k,
                        r,
                        b,
                        segmentation ? "yes" : "no")
                .flatMap(s -> (s instanceof Stream<?> st) ? st : Stream.of(s))
                .map(Objects::toString).toArray(String[]::new);
    }
}
