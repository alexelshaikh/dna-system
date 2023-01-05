package core.dnarules;

import core.BaseSequence;

public interface DNARule {
    /**
     * Maps a given DNA sequence to an error value.
     * @param seq the DNA sequence.
     * @return the error for seq.
     */
    float evalErrorProbability(BaseSequence seq);
}
