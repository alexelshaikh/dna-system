package utils.csv;

import utils.FuncUtils;
import utils.Streamable;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.*;
import java.util.stream.Collectors;

public class BufferedCsvReader extends CsvFile implements Streamable<CsvLine> {
    private final BufferedReader bf;
    private Map<String, Integer> colMapping;

    public BufferedCsvReader(String path, String delim, int buffSize) {
        super(path, delim);
        this.bf = FuncUtils.safeCall(() -> new BufferedReader(new FileReader(path), buffSize));
        this.colMapping = new HashMap<>();

        putColMapping();
    }

    public BufferedCsvReader(String path, String delim) {
        this(path, delim, DEFAULT_BUFF_SIZE);
    }

    public BufferedCsvReader(String path) {
        this(path, CsvLine.DEFAULT_DELIM);
    }

    public int headerSize() {
        return colMapping.size();
    }

    public CsvLine readLine() {
        return new CsvLine(FuncUtils.safeCall(bf::readLine), colMapping);
    }

    public boolean available() {
        return FuncUtils.safeCall(bf::ready);
    }

    private void putColMapping() {
        String[] colNames = FuncUtils.safeCall(() -> this.bf.readLine().split(delim));
        for (int i = 0; i < colNames.length; i++) {
            if (colMapping.containsKey(colNames[i]))
                throw new RuntimeException("duplicate column name: " + colNames[i]);

            colMapping.put(colNames[i], i);
        }
        colMapping = Collections.unmodifiableMap(colMapping);
    }

    public Set<String> getHeaderSet() {
        return colMapping.keySet();
    }

    public String getHeaderLine() {
        return colMapping.entrySet().stream().sorted(Map.Entry.comparingByValue()).map(Map.Entry::getKey).collect(Collectors.joining(delim));
    }

    public String[] getHeaderArray() {
        return colMapping.entrySet().stream().sorted(Map.Entry.comparingByValue()).map(Map.Entry::getKey).toArray(String[]::new);
    }

    public Map<String, Integer> getColMapping() {
        return colMapping;
    }

    @Override
    public Iterator<CsvLine> iterator() {
        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                return available();
            }
            @Override
            public CsvLine next() {
                return readLine();
            }
        };
    }

    @Override
    public void close() {
        FuncUtils.safeRun(this.bf::close);
    }
}
