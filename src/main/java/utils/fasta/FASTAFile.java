package utils.fasta;

import core.BaseSequence;
import java.util.List;

/**
 * The super class of reading and writing fasta files.
 */
public abstract class FASTAFile implements AutoCloseable {
    protected final static String CAPTION_PREFIX = ">";
    protected final static int CAPTION_START_INDEX = CAPTION_PREFIX.length();
    protected final static String LINE_SEPARATOR = "\n";
    protected final static String LINE_SEP_PLUS_CAPTION_PRE = LINE_SEPARATOR + CAPTION_PREFIX;

    protected String path;

    public FASTAFile(String path) {
        this.path = path;
    }

    public static String getCaptionPrefix() {
        return CAPTION_PREFIX;
    }

    public static String getLineSeparator() {
        return LINE_SEPARATOR;
    }

    public static void append(String path, BaseSequence seq) {
        try (WriteableFASTAFile aff = new WriteableFASTAFile(path)) {
            aff.append(seq);
        }
    }

    public static void append(String path, Iterable<BaseSequence> seqs) {
        try (WriteableFASTAFile aff = new WriteableFASTAFile(path)) {
            aff.append(seqs);
        }
    }

    public static List<BaseSequence> readSeqs(String path) {
        try (ReadableFASTAFile rff = new ReadableFASTAFile(path)) {
           return rff.readRemainingSeqs();
        }
    }

    public static List<ReadableFASTAFile.Entry> readEntries(String path) {
        try (ReadableFASTAFile aff = new ReadableFASTAFile(path)) {
            return aff.readRemaining();
        }
    }

    public static void append(String path, BaseSequence seq, String caption) {
        try (WriteableFASTAFile aff = new WriteableFASTAFile(path)) {
            aff.append(seq, caption);
        }
    }

    public String getPath() {
        return path;
    }

}
