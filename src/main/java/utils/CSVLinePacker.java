package utils;

import dnacoders.Coder;
import utils.csv.CsvLine;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public final class CSVLinePacker {

    public static Coder<CsvLine, byte[]> fromTypeByAttributeNameArrays(String[] attributeNames, Packer.Type[] types) {
        return new Coder<>() {
            final Map<String, Integer> colMapping = IntStream.range(0, attributeNames.length).boxed().collect(Collectors.toMap(i -> attributeNames[i], i -> i));
            @Override
            public byte[] encode(CsvLine csvLine) {
                BitString buff = new BitString();
                IntStream.range(0, attributeNames.length).forEach(i -> packSafe(csvLine.get(attributeNames[i]), buff, types[i]));
                return Packer.withBytePadding(buff).toBytes();
            }

            @Override
            public CsvLine decode(byte[] bytes) {
                return new CsvLine(
                        unpackALl(Packer.withoutBytePadding(new BitString().append(bytes))),
                        colMapping
                );
            }
        };
    }

    private static void packSafe(String s, BitString buff, Packer.Type type) {
        FuncUtils.tryOrElse(() -> Packer.pack(s, buff, type), () -> Packer.packString(s, buff));
    }

    private static String[] unpackALl(BitString buff) {
        return Packer.unpackLazy(buff).map(Packer.UnpackingResult::getValue).map(Object::toString).toArray(String[]::new);
    }
}
