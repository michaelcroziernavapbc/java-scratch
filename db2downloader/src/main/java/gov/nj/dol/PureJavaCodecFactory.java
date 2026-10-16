package gov.nj.dol;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

/**
 * A {@link CompressionCodecFactory} with no Hadoop and no JNI behind it.
 *
 * <p>Parquet's built-in {@code org.apache.parquet.hadoop.CodecFactory} cannot even be loaded without
 * hadoop-common on the classpath: one of its private methods returns
 * {@code org.apache.hadoop.io.compress.zlib.ZlibCompressor.CompressionLevel}, and HotSpot verifies a
 * class as a unit, so the missing type fails the whole class at link time. Passing an instance of this
 * class to {@code ParquetWriter.Builder.withCodecFactory} keeps {@code CodecFactory} off the reachable
 * graph entirely.
 *
 * <p>Snappy runs through aircompressor (pure Java) rather than snappy-java, so there is no bundled
 * {@code .so} to unpack at startup — the whole point of shipping a native binary.
 *
 * <p>Only the codecs this tool actually emits are supported; anything else fails loudly rather than
 * silently writing a file no reader expects.
 */
final class PureJavaCodecFactory implements CompressionCodecFactory {

    @Override
    public BytesInputCompressor getCompressor(CompressionCodecName codecName) {
        return switch (codecName) {
            case UNCOMPRESSED -> new NoopCompressor();
            case SNAPPY -> new SnappyCompressor();
            case GZIP -> new GzipCompressor();
            default -> throw unsupported(codecName);
        };
    }

    @Override
    public BytesInputDecompressor getDecompressor(CompressionCodecName codecName) {
        return switch (codecName) {
            case UNCOMPRESSED -> new NoopDecompressor();
            case SNAPPY -> new SnappyDecompressor();
            case GZIP -> new GzipDecompressor();
            default -> throw unsupported(codecName);
        };
    }

    @Override
    public void release() {
    }

    private static UnsupportedOperationException unsupported(CompressionCodecName codecName) {
        return new UnsupportedOperationException(
                codecName + " is not supported by " + PureJavaCodecFactory.class.getSimpleName()
                        + "; use UNCOMPRESSED, SNAPPY or GZIP");
    }

    /** Copies a decompression target back out as a BytesInput, honouring the caller's buffer positions. */
    private static void putDecompressed(ByteBuffer output, byte[] decompressed, int uncompressedSize) {
        output.put(decompressed, 0, uncompressedSize);
    }

    private static byte[] readFully(ByteBuffer input, int length) {
        final var bytes = new byte[length];
        input.get(bytes, 0, length);
        return bytes;
    }

    private static final class NoopCompressor implements BytesInputCompressor {
        @Override
        public BytesInput compress(BytesInput bytes) {
            return bytes;
        }

        @Override
        public CompressionCodecName getCodecName() {
            return CompressionCodecName.UNCOMPRESSED;
        }

        @Override
        public void release() {
        }
    }

    private static final class NoopDecompressor implements BytesInputDecompressor {
        @Override
        public BytesInput decompress(BytesInput bytes, int uncompressedSize) {
            return bytes;
        }

        @Override
        public void decompress(ByteBuffer input, int compressedSize, ByteBuffer output, int uncompressedSize) {
            output.put(readFully(input, compressedSize), 0, uncompressedSize);
        }

        @Override
        public void release() {
        }
    }

    private static final class SnappyCompressor implements BytesInputCompressor {
        private final io.airlift.compress.snappy.SnappyCompressor delegate =
                new io.airlift.compress.snappy.SnappyCompressor();

        @Override
        public BytesInput compress(BytesInput bytes) throws IOException {
            final var input = bytes.toByteArray();
            final var output = new byte[delegate.maxCompressedLength(input.length)];
            final var length = delegate.compress(input, 0, input.length, output, 0, output.length);
            return BytesInput.from(output, 0, length);
        }

        @Override
        public CompressionCodecName getCodecName() {
            return CompressionCodecName.SNAPPY;
        }

        @Override
        public void release() {
        }
    }

    private static final class SnappyDecompressor implements BytesInputDecompressor {
        private final io.airlift.compress.snappy.SnappyDecompressor delegate =
                new io.airlift.compress.snappy.SnappyDecompressor();

        @Override
        public BytesInput decompress(BytesInput bytes, int uncompressedSize) throws IOException {
            final var input = bytes.toByteArray();
            final var output = new byte[uncompressedSize];
            delegate.decompress(input, 0, input.length, output, 0, output.length);
            return BytesInput.from(output);
        }

        @Override
        public void decompress(ByteBuffer input, int compressedSize, ByteBuffer output, int uncompressedSize)
                throws IOException {
            final var compressed = readFully(input, compressedSize);
            final var decompressed = new byte[uncompressedSize];
            delegate.decompress(compressed, 0, compressedSize, decompressed, 0, uncompressedSize);
            putDecompressed(output, decompressed, uncompressedSize);
        }

        @Override
        public void release() {
        }
    }

    private static final class GzipCompressor implements BytesInputCompressor {
        @Override
        public BytesInput compress(BytesInput bytes) throws IOException {
            final var buffer = new ByteArrayOutputStream();
            try (var gzip = new GZIPOutputStream(buffer)) {
                bytes.writeAllTo(gzip);
            }
            return BytesInput.from(buffer);
        }

        @Override
        public CompressionCodecName getCodecName() {
            return CompressionCodecName.GZIP;
        }

        @Override
        public void release() {
        }
    }

    private static final class GzipDecompressor implements BytesInputDecompressor {
        @Override
        public BytesInput decompress(BytesInput bytes, int uncompressedSize) throws IOException {
            return BytesInput.from(inflate(bytes.toByteArray(), uncompressedSize));
        }

        @Override
        public void decompress(ByteBuffer input, int compressedSize, ByteBuffer output, int uncompressedSize)
                throws IOException {
            putDecompressed(output, inflate(readFully(input, compressedSize), uncompressedSize), uncompressedSize);
        }

        private static byte[] inflate(byte[] compressed, int uncompressedSize) throws IOException {
            final var output = new byte[uncompressedSize];
            try (var gzip = new GZIPInputStream(new ByteArrayInputStream(compressed))) {
                gzip.readNBytes(output, 0, uncompressedSize);
            }
            return output;
        }

        @Override
        public void release() {
        }
    }
}
