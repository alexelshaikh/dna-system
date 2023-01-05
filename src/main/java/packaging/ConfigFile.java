package packaging;

import org.json.JSONObject;
import utils.FuncUtils;
import utils.Packer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ConfigFile {

    private static final String TABLE_PATH                      = "table_path";
    private static final String MODE                            = "mode";
    private static final String ENCODE_PATH                     = "encode_path";
    private static final String DECODE_PATH                     = "decode_path";
    private static final String PARALLEL                        = "parallel";
    private static final String RQ_MAX_ERROR                    = "rq_max_error";
    private static final String SEGMENTATION_LENGTH             = "L_{Info}";
    private static final String SEGMENTATION_PERMUTATIONS       = "segmentation_permutations";
    private static final String SEGMENTATION_GC_CORRECTIONS     = "segmentation_gc_corrections";


    private static final String CBBs                            = "content-based_barcodes";
    private static final String CBBs_PERMUTATIONS               = "permutations";
    private static final String CBBs_MINIMUM_LENGTH             = "L_{CBB}";
    private static final String CBBs_DNA_CONVERTOR              = "dna_convertor";

    private static final String UNIQUE_ATTRIBUTES               = "unique_attributes";
    private static final String NON_UNIQUE_ATTRIBUTES           = "non-unique_attributes";

    private static final String ATTRIBUTE_KEY                   = "key";
    private static final String ATTRIBUTE_MAPPING               = "mapping";
    private static final String ATTRIBUTE_SEGMENTATION          = "segmentation";
    private static final String ATTRIBUTE_GZIP                  = "gzip";


    private static final String LSH                             = "lsh";
    private static final String LSH_k                           = "k";
    private static final String LSH_r                           = "r";
    private static final String LSH_b                           = "b";

    private static final String DISTANCE_CODER                  = "distance_coder";
    private static final String DISTANCE_CODER_C_ERROR          = "c_error";
    private static final String DISTANCE_CODER_C_DISTANCE       = "c_distance";


    private final JSONObject params;

    public ConfigFile(String configPath) {
        this.params = new JSONObject(FuncUtils.safeCall(() -> Files.readAllLines(Path.of(configPath)).stream().filter(l -> !l.matches("[ \t]*#.*")).collect(Collectors.joining())));
    }



    public APP_MODE getMode() {
        String mode = params.getString(MODE);
        APP_MODE appMode = FuncUtils.superSafeCall(() -> APP_MODE.valueOf(mode));
        if (appMode == null)
            throw new RuntimeException("The specified mode: " + mode + " is not valid. Possible modes: \"encode\", \"decode\", or \"both\".");

        return appMode;
    }

    public String getTablePath() {
        return params.getString(TABLE_PATH);
    }

    public String getEncodePath() {
        return params.getString(ENCODE_PATH);
    }

    public String getDecodePath() {
        return params.getString(DECODE_PATH);
    }

    public boolean getParallel() {
        return params.getBoolean(PARALLEL);
    }

    public float getRqMaxError() {
        return params.getFloat(RQ_MAX_ERROR);
    }

    public int getSegmentationLength() {
        return params.getInt(SEGMENTATION_LENGTH);
    }

    public int getSegmentationPermutations() {
        return params.getInt(SEGMENTATION_PERMUTATIONS);
    }

    public int getSegmentationGcCorrections() {
        return params.getInt(SEGMENTATION_GC_CORRECTIONS);
    }

    public int getCBBsPermutations() {
        return params.getJSONObject(CBBs).getInt(CBBs_PERMUTATIONS);
    }

    public int getCBBsMinimumLength() {
        return params.getJSONObject(CBBs).getInt(CBBs_MINIMUM_LENGTH);
    }

    public String getCBBsDnaConvertor() {
        return params.getJSONObject(CBBs).getString(CBBs_DNA_CONVERTOR);
    }

    public int getLshK() {
        return params.getJSONObject(LSH).getInt(LSH_k);
    }

    public int getLshR() {
        return params.getJSONObject(LSH).getInt(LSH_r);
    }

    public int getLshB() {
        return params.getJSONObject(LSH).getInt(LSH_b);
    }

    public float getDistanceCoderCError() {
        return params.getJSONObject(DISTANCE_CODER).getFloat(DISTANCE_CODER_C_ERROR);
    }

    public float getDistanceCoderCDistance() {
        return params.getJSONObject(DISTANCE_CODER).getFloat(DISTANCE_CODER_C_DISTANCE);
    }

    public List<UniqueAttribute> getUniqueAttributes() {
        return FuncUtils.stream(() -> params.getJSONArray(UNIQUE_ATTRIBUTES).iterator()).map(o -> (JSONObject) o).map(UniqueAttribute::new).toList();
    }

    public List<NonUniqueAttribute> getNonUniqueAttributes() {
        return FuncUtils.stream(() -> params.getJSONArray(NON_UNIQUE_ATTRIBUTES).iterator()).map(o -> (JSONObject) o).map(NonUniqueAttribute::new).toList();
    }

    @Override
    public String toString() {
        String prefix = "-> ";
        int indents = 1;
        return prefix + MODE + ": " + getMode() + "\n" +
                prefix + PARALLEL + ": " + getParallel() + "\n" +
                prefix + TABLE_PATH + ": " + getTablePath() + "\n" +
                prefix + ENCODE_PATH + ": " + getEncodePath() + "\n" +
                prefix + DECODE_PATH + ": " + getDecodePath() + "\n" +
                "-----------------------------\n" +
                prefix + RQ_MAX_ERROR + ": " + getRqMaxError() + "\n" +
                prefix + SEGMENTATION_LENGTH + ": " + getSegmentationLength() + "\n" +
                prefix + SEGMENTATION_PERMUTATIONS + ": " + getSegmentationPermutations() + "\n" +
                prefix + SEGMENTATION_GC_CORRECTIONS + ": " + getSegmentationGcCorrections() + "\n" +
                "-----------------------------\n" +
                prefix + CBBs + "\n" +
                "\t" + CBBs_MINIMUM_LENGTH + ": " + getCBBsMinimumLength() + "\n" +
                "\t" + CBBs_PERMUTATIONS + ": " + getCBBsPermutations() + "\n" +
                "\t" + CBBs_DNA_CONVERTOR + ": " + getCBBsDnaConvertor() + "\n" +
                "-----------------------------\n" +
                prefix + UNIQUE_ATTRIBUTES + "\n" +
                getUniqueAttributes().stream().map(a -> a.withIndents(indents)).collect(Collectors.joining("\n" + "\t".repeat(indents) + "--------------\n")) + "\n" +
                "-----------------------------\n" +
                prefix + NON_UNIQUE_ATTRIBUTES + "\n" +
                getNonUniqueAttributes().stream().map(a -> a.withIndents(indents)).collect(Collectors.joining("\n" + "\t".repeat(indents) + "--------------\n")) + "\n" +
                "-----------------------------\n" +
                prefix + LSH + "\n" +
                "\t" + LSH_k + ": " + getLshK() + "\n" +
                "\t" + LSH_r + ": " + getLshR() + "\n" +
                "\t" + LSH_b + ": " + getLshB() + "\n" +
                "-----------------------------\n" +
                prefix + DISTANCE_CODER + "\n" +
                "\t" + DISTANCE_CODER_C_ERROR + ": " + getDistanceCoderCError() + "\n" +
                "\t" + DISTANCE_CODER_C_DISTANCE + ": " + getDistanceCoderCDistance() + "\n" +
                "-----------------------------------------------------------\n";
    }


    public static class UniqueAttribute extends Attribute {
        private final String[] mappingAttributes;
        private final Packer.Type[] mappingAttributesTypes;

        public UniqueAttribute(JSONObject jo) {
            super(
                    jo.getString(ATTRIBUTE_KEY),
                    jo.getBoolean(ATTRIBUTE_SEGMENTATION),
                    jo.getBoolean(ATTRIBUTE_GZIP)
            );
            JSONObject mappings = jo.getJSONObject(ATTRIBUTE_MAPPING);
            Set<String> keySet = mappings.keySet();
            Iterator<String> keys = keySet.iterator();
            this.mappingAttributes = new String[mappings.length()];
            this.mappingAttributesTypes = new Packer.Type[mappings.length()];
            int keySetSize = keySet.size();
            String key;
            for (int i = 0; i < keySetSize; i++) {
                key = keys.next();
                mappingAttributes[i] = key;
                mappingAttributesTypes[i] = Packer.Type.valueOf(mappings.getString(key).toUpperCase(Locale.ROOT));
            }
        }

        public String[] getMappingAttributes() {
            return mappingAttributes;
        }

        public Packer.Type[] getMappingAttributesTypes() {
            return mappingAttributesTypes;
        }

        @Override
        public String toString() {
            return "UniqueAttribute{" +
                    "key='" + key + "\', " +
                    "mappingAttributes=[" + IntStream.range(0, mappingAttributes.length).mapToObj(i -> mappingAttributes[i] + ": " + mappingAttributesTypes[i]).collect(Collectors.joining(", ")) + "]\n" +
                    ", segmentation=" + segmentation +
                    ", gzip=" + gzip +
                    '}';
        }

        public String withIndents(int c) {
            String indents = "\t".repeat(c);
            return super.withIndents(c) + "\n"
                    + indents + "mappingAttributes: [" + IntStream.range(0, mappingAttributes.length).mapToObj(i -> mappingAttributes[i] + ": " + mappingAttributesTypes[i]).collect(Collectors.joining(", ")) + "]\n";
        }
    }


    public static class NonUniqueAttribute extends Attribute {
        private final String mappingAttribute;

        public NonUniqueAttribute(JSONObject jo) {
            super(
                    jo.getString(ATTRIBUTE_KEY),
                    jo.getBoolean(ATTRIBUTE_SEGMENTATION),
                    jo.getBoolean(ATTRIBUTE_GZIP)
            );
            this.mappingAttribute = jo.getString(ATTRIBUTE_MAPPING);
        }

        public String getMappingAttribute() {
            return mappingAttribute;
        }

        @Override
        public String toString() {
            return "NonUniqueAttribute{" +
                    "mappingAttribute='" + mappingAttribute + '\'' +
                    ", key='" + key + '\'' +
                    ", segmentation=" + segmentation +
                    ", gzip=" + gzip +
                    '}';
        }

        public String withIndents(int c) {
            String indents = "\t".repeat(c);
            return super.withIndents(c) + "\n"
                    + indents + "mapping: " + mappingAttribute;
        }
    }


    public abstract static class Attribute {
        protected String key;
        protected boolean segmentation;
        protected boolean gzip;

        protected Attribute(String key, boolean segmentation, boolean gzip) {
            this.key = key;
            this.segmentation = segmentation;
            this.gzip = gzip;
        }


        public String getKey() {
            return key;
        }

        public boolean getSegmentation() {
            return segmentation;
        }

        public boolean getGZIP() {
            return gzip;
        }

        public String withIndents(int c) {
            String indents = "\t".repeat(c);
            return indents + "key: " + key + "\n"
                    + indents + "segmentation: " + segmentation + "\n"
                    + indents + "gzip: " + gzip;
        }
    }

    public enum APP_MODE {
        encode, decode, both;

        public boolean doEncode() {
            return this == encode || this == both;
        }

        public boolean doDecode() {
            return this == decode || this == both;
        }
    }
}
