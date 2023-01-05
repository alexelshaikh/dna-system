package utils.csv;

import utils.FuncUtils;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.Flushable;

public class BufferedCsvWriter extends CsvFile implements Flushable {
    private final BufferedWriter bw;
    private boolean isEmpty;

    public BufferedCsvWriter(String path, String delim, int buffSize, boolean append) {
        super(path, delim);
        this.isEmpty = !append || new File(path).length() <= 0;
        this.bw = FuncUtils.safeCall(() -> new BufferedWriter(new FileWriter(path, append), buffSize));
    }

    public BufferedCsvWriter(String path, String delim) {
        this(path, delim, true);
    }

    public BufferedCsvWriter(String path) {
        this(path, true);
    }

    public BufferedCsvWriter(String path, boolean append) {
        this(path, CsvLine.DEFAULT_DELIM, DEFAULT_BUFF_SIZE, append);
    }

    public BufferedCsvWriter(String path, String delim, boolean append) {
        this(path, delim, DEFAULT_BUFF_SIZE, append);
    }

    public void appendNewLine(String... values) {
        if (isEmpty)
            FuncUtils.safeRun(() -> this.bw.write(String.join(delim, values)));
        else
            FuncUtils.safeRun(() -> this.bw.write(LINE_SEPARATOR + String.join(delim, values)));

        isEmpty = false;
    }

    public void newLine() {
        FuncUtils.safeRun(() -> this.bw.write(LINE_SEPARATOR));
        isEmpty = false;
    }

    public boolean isEmpty() {
        return isEmpty;
    }

    @Override
    public void flush() {
        FuncUtils.safeRun(this.bw::flush);
    }

    @Override
    public void close() {
        FuncUtils.safeRun(this.bw::close);
    }
}
