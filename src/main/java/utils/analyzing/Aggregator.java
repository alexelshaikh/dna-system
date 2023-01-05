package utils.analyzing;

import utils.FuncUtils;
import java.util.*;
import java.util.stream.DoubleStream;


public class Aggregator {

    /**
     * @param numbers the list of numbers to aggregate.
     * @param parallel true to compute the aggregate in parallel, and false to compute it sequentially.
     * @return the aggregate.
     */
    public static NumberAggregates aggregateNumbers(List<? extends Number> numbers, boolean parallel) {
        DoubleSummaryStatistics summary = FuncUtils.stream(numbers.stream(), parallel).mapToDouble(Number::doubleValue).summaryStatistics();
        double avg = summary.getAverage();
        long count = summary.getCount();
        double var = FuncUtils.stream(numbers.stream(), parallel).mapToDouble(d -> Math.pow(d.doubleValue() - avg, 2)).sum() / count;
        double standardDev = Math.sqrt(var);

        return new NumberAggregates(
                count,
                summary.getMin(),
                summary.getMax(),
                avg,
                summary.getSum(),
                var,
                standardDev
        );
    }

    /**
     * @param numbers the array of numbers to aggregate.
     * @param parallel true to compute the aggregate in parallel, and false to compute it sequentially.
     * @return the aggregate.
     */
    public static NumberAggregates aggregateNumbers(double[] numbers, boolean parallel) {
        DoubleSummaryStatistics summary = FuncUtils.stream(DoubleStream.of(numbers), parallel).summaryStatistics();
        double avg = summary.getAverage();
        long count = summary.getCount();
        double var = FuncUtils.stream(DoubleStream.of(numbers), parallel).map(d -> Math.pow(d - avg, 2)).sum() / count;

        return new NumberAggregates(
                count,
                summary.getMin(),
                summary.getMax(),
                avg,
                summary.getSum(),
                var,
                Math.sqrt(var)
        );
    }

    public record NumberAggregates(long count, double min, double max, double avg, double sum, double var, double stdDev) {
    }
}
