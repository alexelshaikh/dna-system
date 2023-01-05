package dnacoders.headercoders;

import core.BaseSequence;
import dnacoders.DNACoder;

public interface HeaderCoder<H> extends DNACoder<BaseSequence> {
    /**
     * Returns the decoded header information.
     * @param encoded the encoded DNA sequence.
     * @return the decoded header.
     */
    H decodeHeader(BaseSequence encoded);

    /**
     * @param encoded the encoded DNA sequence.
     * @return the DNA sequence containing the header information.
     */
    BaseSequence extractPayload(BaseSequence encoded);

    /**
     * Decodes the given payload and header to a DNA sequence.
     * @param payload the DNA sequence without header information.
     * @param header the DNA sequence encodig the header information.
     * @return the decoded DNA sequence.
     */
    BaseSequence decode(BaseSequence payload, H header);

    @Override
    default BaseSequence decode(BaseSequence encoded) {
        return decode(extractPayload(encoded), decodeHeader(encoded));
    }
}