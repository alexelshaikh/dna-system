package utils.compression;

import dnacoders.Coder;
import utils.FuncUtils;
import java.io.ByteArrayOutputStream;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.Inflater;
import java.util.zip.InflaterOutputStream;

/**
 * The class that enables GZIP compression.
 */
public class GZIP implements Coder<byte[], byte[]> {

    public static final GZIP INSTANCE = new GZIP();

    public byte[] encode(byte[] data) {
        return FuncUtils.safeCall(() -> {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            DeflaterOutputStream stream = new DeflaterOutputStream(out, new Deflater(Deflater.BEST_COMPRESSION, true));
            stream.write(data);
            stream.flush();
            stream.close();
            return out.toByteArray();
        });
    }

    public byte[] decode(byte[] data) {
        return FuncUtils.safeCall(() -> {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            InflaterOutputStream stream = new InflaterOutputStream(out, new Inflater(true));
            stream.write(data);
            stream.flush();
            stream.close();
            return out.toByteArray();
        });
    }
}