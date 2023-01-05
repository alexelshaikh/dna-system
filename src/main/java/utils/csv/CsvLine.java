package utils.csv;

import java.util.Map;
import java.util.stream.Collectors;

public class CsvLine {

    public static final String DEFAULT_DELIM = ",";

    private final String line;
    private final String[] splitLine;
    private final Map<String, Integer> colMapping;

    public CsvLine(String line, Map<String, Integer> colMapping, String delim) {
        this.line = line;
        this.splitLine = line.split(delim);
        this.colMapping = colMapping;
    }

    public CsvLine(String line, Map<String, Integer> colMapping) {
        this(line, colMapping, DEFAULT_DELIM);
    }

    public CsvLine(String[] splitLine, Map<String, Integer> colMapping) {
        this.line = String.join(DEFAULT_DELIM, splitLine);
        this.splitLine = splitLine;
        this.colMapping = colMapping;
    }

    public String get(String colName, Map<String, Integer> colMapping) {
        Integer index = colMapping.get(colName);
        if (index == null)
            throw new RuntimeException("column not found: " + colName);

        return get(index);
    }

    public String get(String colName) {
        return get(colName, colMapping);
    }

    public String get(int index) {
        if (index < 0 || index > splitLine.length)
            throw new RuntimeException("index out of bounds: " + index);

        return splitLine[index];
    }

    public String getLine() {
        return line;
    }

    public String[] getSplitLine() {
        return splitLine;
    }

    public Map<String, Integer> getColMapping() {
        return colMapping;
    }

    @Override
    public String toString() {
        return colMapping.entrySet().stream().map(e -> e.getKey() + ": " + splitLine[e.getValue()]).collect(Collectors.joining(", "));
    }
}
