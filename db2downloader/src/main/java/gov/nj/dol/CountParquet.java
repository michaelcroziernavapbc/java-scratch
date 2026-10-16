package gov.nj.dol;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;

import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.Util;

/**
 * Prints the number of rows in a parquet file.
 *
 * <p>Reads only the footer, so it is fast regardless of file size. It parses the footer directly
 * rather than going through {@code ParquetFileReader} because even {@code ParquetReadOptions.builder()}
 * eagerly constructs Hadoop's {@code CodecFactory}, which cannot load without hadoop-common.
 */
public class CountParquet {

    // A parquet file ends with: <footer> <4-byte little-endian footer length> "PAR1"
    private static final byte[] MAGIC = "PAR1".getBytes(StandardCharsets.US_ASCII);
    private static final int TAIL_LENGTH = Integer.BYTES + MAGIC.length;

    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            System.err.println("Usage: CountParquet <file.parquet>");
            System.exit(1);
        }
        System.out.println(countRows(Path.of(args[0])));
    }

    static long countRows(Path path) throws IOException {
        try (var channel = FileChannel.open(path)) {
            final long size = channel.size();
            if (size < MAGIC.length + TAIL_LENGTH) {
                throw new IOException(path + " is too small to be a parquet file");
            }

            final var tail = readFully(channel, size - TAIL_LENGTH, TAIL_LENGTH)
                    .order(ByteOrder.LITTLE_ENDIAN);
            final int footerLength = tail.getInt();
            final var magic = new byte[MAGIC.length];
            tail.get(magic);
            if (!Arrays.equals(magic, MAGIC)) {
                throw new IOException(path + " is not a parquet file (or uses an encrypted footer)");
            }

            final var footer = readFully(channel, size - TAIL_LENGTH - footerLength, footerLength);
            final FileMetaData metadata = Util.readFileMetaData(
                    new ByteArrayInputStream(footer.array()));
            return metadata.getNum_rows();
        }
    }

    private static ByteBuffer readFully(FileChannel channel, long position, int length) throws IOException {
        final var buffer = ByteBuffer.allocate(length);
        while (buffer.hasRemaining()) {
            if (channel.read(buffer, position + buffer.position()) < 0) {
                throw new IOException("Unexpected end of file");
            }
        }
        return buffer.flip();
    }
}
