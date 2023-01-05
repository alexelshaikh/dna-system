package utils;

import dnacoders.AttributeMapper;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class ByteFrequencyLoader {

    public static final String DEFAULT_FREQS_PATH = System.getProperty("user.dir") + "/freqs.txt";

    public static Map<Byte, Integer> load(String path) throws IllegalArgumentException {
        try {
            File f = new File(path);
            if (!f.exists())
                return null;
            BufferedReader reader = new BufferedReader(new FileReader(f));
            String line;
            String[] line_splitted;
            Map<Byte, Integer> freqs = new HashMap<>();
            while ((line = reader.readLine()) != null) {
                line_splitted = line.toLowerCase().replaceAll("[ \t]", "").split("=");
                if (line_splitted.length == 2)
                    freqs.put(Byte.valueOf(line_splitted[0]), Integer.parseInt(line_splitted[1]));
            }

            return freqs;
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed loading freqs from: " + path);
        }
    }


    public static Map<Byte, Integer> load() throws IllegalArgumentException {
        return load(DEFAULT_FREQS_PATH);

    }

    public static Map<Byte, Integer> writeFreqs() {
        return writeFreqs(DEFAULT_FREQS_PATH);
    }

    public static Map<Byte, Integer> loadOrGenerateFreqs() {
        return loadOrGenerateFreqs(DEFAULT_FREQS_PATH);
    }

    public static Map<Byte, Integer> loadOrGenerateFreqs(String path) {
        Map<Byte, Integer> freqs = load(path);
        if (freqs == null || freqs.isEmpty()) {
            return writeFreqs(path);
        }
        return freqs;
    }


    public static Map<Byte, Integer> writeFreqs(String path) {
        final Map<Byte, Integer> FREQS = new TreeMap<>();
        putCharacterFreqs((byte) 'a', FREQS);
        putCharacterFreqs((byte) 'A', FREQS);

        for (byte b = 0; b < 'A'; b++)
            FREQS.put(b, 500);
        for (byte b = 'Z' + 1; b < 'a'; b++)
            FREQS.put(b, 500);
        for (int b = 'z' + 1; b < 256; b++)
            FREQS.put((byte) b, 500);


        FREQS.put((byte) AttributeMapper.MAPPING_STRING.charAt(0), 5000);


        FuncUtils.safeRun(() -> Files.write(
                Path.of(path),
                FREQS.entrySet()
                .stream()
                .map(e -> e.getKey().toString() + "=" + e.getValue().toString())
                .collect(Collectors.joining("\n")).getBytes()));

        return FREQS;

    }

    private static void putCharacterFreqs(byte c, Map<Byte, Integer> FREQS) {
        FREQS.put(c++, 8167);
        FREQS.put(c++, 1492);
        FREQS.put(c++, 2202);
        FREQS.put(c++, 4253);
        FREQS.put(c++, 12702);
        FREQS.put(c++, 2228);
        FREQS.put(c++, 2015);
        FREQS.put(c++, 6094);
        FREQS.put(c++, 6966);
        FREQS.put(c++, 153);
        FREQS.put(c++, 1292);
        FREQS.put(c++, 4025);
        FREQS.put(c++, 2406);
        FREQS.put(c++, 6749);
        FREQS.put(c++, 7507);
        FREQS.put(c++, 1929);
        FREQS.put(c++, 95);
        FREQS.put(c++, 5987);
        FREQS.put(c++, 7000); //FREQS.put(c++, 6327); // fixes adapters F and R issues --> they (plain, reverse, compliment, reverse compliment) can not be decoded
        FREQS.put(c++, 9356);
        FREQS.put(c++, 2758);
        FREQS.put(c++, 978);
        FREQS.put(c++, 2560);
        FREQS.put(c++, 150);
        FREQS.put(c++, 1994);
        FREQS.put(c, 77);
    }
}
