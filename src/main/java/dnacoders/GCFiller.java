package dnacoders;

import core.Base;
import core.BaseSequence;
import utils.FuncUtils;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class GCFiller {
	public static final TreeMap<Float, BaseSequence>    FILLER_MAP                  = loadOrWriteFillerSeqs();
	public static final float                           DEFAULT_TARGET_GC_CONTENT   = 0.5f;
	public static final String                          DEFAULT_FILLER_INDEX_PATH   = "filler.index";
	public static final String                          DEFAULT_FILLER_MAPPING      = "=";
	public static final int                             FILLER_LENGTH               = 1000;


	/**
	 * Computes the padding DNA sequence for a given DNA sequence, the remaining bases, and the target gc content.
	 * @param seq the DNA sequence to be padded.
	 * @param remaining the number of bases missing.
	 * @param targetGc the desired gc content of seq after padding.
	 * @return the padding sequence.
	 */
	public static BaseSequence getFiller(BaseSequence seq, int remaining, float targetGc) {
		if (remaining > FILLER_LENGTH)
			throw new RuntimeException("Only filling up to " + FILLER_LENGTH + " bases!");
		float index = ((seq.length() + remaining) * targetGc - seq.gcCount()) / remaining;
		return FILLER_MAP.floorEntry(Math.max(0, index)).getValue();
	}

	/**
	 * Computes the padding DNA sequence for a given DNA sequence, and the remaining bases.
	 * @param seq the DNA sequence to be padded.
	 * @param remaining the number of bases missing.
	 * @return the padding sequence to achieve a target gc content of 50%.
	 */
	public static BaseSequence getFiller(BaseSequence seq, int remaining) {
		return getFiller(seq, remaining, DEFAULT_TARGET_GC_CONTENT);
	}

	/**
	 * Computes the trimmed padding DNA sequence for a given DNA sequence, the remaining bases, and the target gc content.
	 * @param seq the DNA sequence to be padded.
	 * @param remaining the number of bases missing.
	 * @param targetGc the desired gc content of seq after padding.
	 * @return the padding sequence only containing the number of DNA bases remaining.
	 */
	public static BaseSequence getTrimmedFiller(BaseSequence seq, int remaining, float targetGc) {
		BaseSequence filler = getFiller(seq, remaining, targetGc);
		int startIndex = Math.abs((seq.hashCode() % (filler.length() - seq.length() - remaining)));
		int endIndex = startIndex + remaining;
		return filler.subSequence(startIndex, endIndex);
	}

	/**
	 * Computes the trimmed padding DNA sequence for a given DNA sequence, and the remaining bases.
	 * @param seq the DNA sequence to be padded.
	 * @param remaining the number of bases missing.
	 * @return the padding sequence only containing the number of DNA bases remaining for a target gc content of 50%.
	 */
	public static BaseSequence getTrimmedFiller(BaseSequence seq, int remaining) {
		return getTrimmedFiller(seq, remaining, DEFAULT_TARGET_GC_CONTENT);
	}


	/**
	 * Loads all padding sequences if available on disk, and computes them if not available.
	 */
	public static TreeMap<Float, BaseSequence> loadOrWriteFillerSeqs() {
		TreeMap<Float, BaseSequence> map;
		Path p  = Path.of(DEFAULT_FILLER_INDEX_PATH);
		if (Files.exists(p)) {
			map = FuncUtils.safeCall(() -> Files.readAllLines(p)).stream().map(l -> l.split(DEFAULT_FILLER_MAPPING)).collect(Collectors.toMap(s -> Float.parseFloat(s[0]), s -> new BaseSequence(s[1]), (s1, s2) -> s2, TreeMap::new));
			if (map.values().stream().mapToInt(BaseSequence::length).allMatch(l -> l == FILLER_LENGTH))
				return map;
		}

		map = new TreeMap<>();
		for (int i = 0; i <= 100; i += 10) {
			float finalI = i / 100f;
			BaseSequence seq = Stream.generate(() -> Base.randomGC(finalI)).limit(FILLER_LENGTH).collect(BaseSequence.COLLECTOR_BASE);
			map.put(finalI, seq);
		}
		TreeMap<Float, BaseSequence> finalMap = map;
		FuncUtils.safeRun(() -> Files.writeString(p, finalMap.entrySet().stream().map(e -> e.getKey() + DEFAULT_FILLER_MAPPING + e.getValue()).collect(Collectors.joining("\n"))));
		return map;
	}
}
