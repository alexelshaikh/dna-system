package dnacoders;

import core.Attribute;
import utils.FuncUtils;

/**
 * AttributeMapper is an attribute's coder that keeps the encoding as short as possible and encodes an attribute simply by:
 * 1. Mapping the attribute onto a String as <code>"key=value"</code>.
 * 2. Flipping this String to <code>"eulav=yek"</code>.
 * 3. Encoding this String to a BaseSequence with the given DNA convertor.
 */
public class AttributeMapper {

    public static final String MAPPING_STRING = "=";

    /**
     * Creates an attribute's mapper with a given DNA convertor.
     * @param dnaConvertor the DNA convertor.
     * @return the attribute's mapper.
     */
    public static DNACoder<Attribute<?>> newInstance(DNACoder<String> dnaConvertor) {
        return DNACoder.fuse(
                new Coder<Attribute<?>, String>() {
                    @Override
                    public String encode(Attribute<?> attribute) {
                        return attribute.getName() + MAPPING_STRING + attribute.getValue();
                    }
                    @Override
                    public Attribute<?> decode(String s) {
                        return parse(s);
                    }
                },
                new Coder<>() {
                    @Override
                    public String encode(String s) {
                        return FuncUtils.reverseCharSequence(s).toString();
                    }
                    @Override
                    public String decode(String s) {
                        return FuncUtils.reverseCharSequence(s).toString();
                    }
                },
                dnaConvertor
        );
    }


    private static Attribute<?> parse(String s) {
        if (s == null)
            throw new RuntimeException("failed parsing attribute from null string");

        String[] split = s.split(MAPPING_STRING);
        if (split.length != 2)
            throw new RuntimeException("failed parsing attribute: " + s);

        return new Attribute<>(split[0], split[1]);
    }
}
