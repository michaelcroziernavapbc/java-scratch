package gov.nj.dol;

import com.google.gson.Gson;

import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.avro.Schema;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.sql.ResultSetMetaData;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/*
SELECT * FROM OTDUIMP.UIMT_SG02_PAYMENT WHERE SG02_DTE_CLAIM_UIB >= '2025-01-01'
Time taken for segment SG02: 275935 ms

SELECT * FROM OTDUIMP.UIMT_SG03_EMPLOYER WHERE SG03_DTE_CLAIM_UIB >= '2025-01-01'
Time taken for segment SG03: 55851 ms

SELECT * FROM OTDUIMP.UIMT_SG05_EXTENSION WHERE SG05_DTE_CLAIM_UIB >= '2025-01-01'
Time taken for segment SG05: 716 ms

SELECT * FROM OTDUIMP.UIMT_SG07_MONETARY_HISTORY WHERE SG07_DTE_CLAIM_UIB >= '2025-01-01'
Time taken for segment SG07: 3406 ms

SELECT * FROM OTDUIMP.UIMT_SG10_REAS_ADDL WHERE SG10_DTE_CLAIM_UIB >= '2025-01-01'
Time taken for segment SG10: 5528 ms

SELECT * FROM OTDUIMP.UIMT_SG11_NONMON WHERE SG11_DTE_CLAIM_UIB >= '2025-01-01'
Time taken for segment SG11: 90805 ms

SELECT * FROM OTDUIMP.UIMT_SG15_DEPENDENCY WHERE SG15_DTE_CLAIM_UIB >= '2025-01-01'
Time taken for segment SG15: 555 ms

SELECT * FROM OTDUIMP.UIMT_SG16_REMARKS WHERE SG16_DTE_CLAIM_UIB >= '2025-01-01'
Time taken for segment SG16: 336413 ms

SELECT * FROM OTDUIMP.UIMT_SG04_EMP_CHARGE WHERE SG04_DTE_CLAIM_UIB >= '2025-01-01'
Time taken for segment SG04: 276393 ms

SELECT * FROM OTDUIMP.UIMT_SG17_26B_FORM WHERE SG17_DTE_CLAIM_UIB >= '2025-01-01'
Time taken for segment SG17: 32043 ms

SELECT * FROM OTDUIMP.UIMT_SG18_26B_ROUTING WHERE SG18_DTE_CLAIM_UIB >= '2025-01-01'
Time taken for segment SG18: 21305 ms

SELECT * FROM OTDUIMP.UIMT_SG19_EMP_289D WHERE SG19_DTE_CLAIM_UIB >= '2025-01-01'
Time taken for segment SG19: 280 ms

20m
 */

public class DB2Downloader {

    private final Config config;
    private final List<SegmentInfo> segmentsAndQueries = List.of(
            new SegmentInfo("SG01", "UIMT_SG01_CLAIMANT"),
            new SegmentInfo("SG02", "UIMT_SG02_PAYMENT"),
            new SegmentInfo("SG03", "UIMT_SG03_EMPLOYER"),
            new SegmentInfo("SG05", "UIMT_SG05_EXTENSION"),
            new SegmentInfo("SG07", "UIMT_SG07_MONETARY_HISTORY"),
            new SegmentInfo("SG10", "UIMT_SG10_REAS_ADDL"),
            new SegmentInfo("SG11", "UIMT_SG11_NONMON"),
            new SegmentInfo("SG15", "UIMT_SG15_DEPENDENCY"),
            new SegmentInfo("SG16", "UIMT_SG16_REMARKS"),
            new SegmentInfo("SG04", "UIMT_SG04_EMP_CHARGE"),
            new SegmentInfo("SG17", "UIMT_SG17_26B_FORM"),
            new SegmentInfo("SG18", "UIMT_SG18_26B_ROUTING"),
            new SegmentInfo("SG19", "UIMT_SG19_EMP_289D")
    );

    private final String dateToday = java.time.LocalDate.now().toString();

    public static void main(String[] args) throws Exception {
        final var config = loadConfig(args[0]);
        var db2Downloader = new DB2Downloader(config);

        db2Downloader.downloadAllSegments();
    }

    private void downloadAllSegments() throws SQLException, IOException {
        final var connection = getDB2Connection(config.url(), config.username(), config.password());
        for (var segmentInfo : segmentsAndQueries) {
            long startTime = System.nanoTime();

            String query = formatQueryForSegment(segmentInfo);
            String outputPath = formatOutputPathForSegment(segmentInfo);
            downloadQueryResults(connection, query, outputPath);

            long endTime = System.nanoTime();
            System.out.println("Time taken for segment " + segmentInfo.segmentName + ": " + (endTime - startTime) / 1_000_000 + " ms");
        }
    }

    private String formatOutputPathForSegment(SegmentInfo segmentInfo) {
        return "output/%s-%s.parquet".formatted(segmentInfo.segmentName, dateToday);
    }

    private String formatQueryForSegment(SegmentInfo segmentInfo) {
        return "SELECT * FROM OTDUIMP.%s WHERE %s_DTE_CLAIM_UIB >= '%s'".formatted(segmentInfo.tableName, segmentInfo.segmentName, config.sinceDate);
    }


    public DB2Downloader(Config config) {
        this.config = config;
        System.out.println(config);
    }

    public void downloadQueryResults(Connection connection, String query, String outputPath) throws SQLException, IOException {
        System.out.println(query);
        System.out.println(outputPath);
        try (final var resultSet = executeQuery(connection, query)) {
            writeResultSetToParquet(resultSet, outputPath);
        }
    }

    private void writeResultSetToParquet(ResultSet resultSet, String outputPath) throws IOException, SQLException {
        final var metaData = resultSet.getMetaData();
        final var avroSchema = createAvroSchema(metaData);
        final var outputFile = new LocalOutputFile(Path.of(outputPath));

        try (ParquetWriter<GenericRecord> writer = buildParquetWriter(outputFile, avroSchema)) {
            while (resultSet.next()) {
                final var record = new GenericData.Record(avroSchema);
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    record.put(metaData.getColumnName(i), resultSet.getObject(i));
                }
                writer.write(record);
            }
        }
    }

    private ParquetWriter<GenericRecord> buildParquetWriter(LocalOutputFile outputFile, Schema avroSchema) throws IOException {
        final var conf = new PlainParquetConfiguration();
        return AvroParquetWriter
                .<GenericRecord>builder(outputFile)
                .withSchema(avroSchema)
                .withConf(conf)
                // Without an explicit model, AvroWriteSupport resolves one via Hadoop's ReflectionUtils.
                .withDataModel(GenericData.get())
                .withCodecFactory(new PureJavaCodecFactory())
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withRowGroupRowCountLimit(config.batchSize())
                .build();
    }

    public Connection getDB2Connection(String url, String username, String password) throws SQLException {
        try {
            Class.forName("com.ibm.db2.jcc.DB2Driver");
        } catch (ClassNotFoundException e) {
            throw new SQLException("DB2 JDBC Driver not found", e);
        }
        return DriverManager.getConnection(url, username, password);
    }

    public ResultSet executeQuery(Connection connection, String query) throws SQLException {
        final var statement = connection.prepareStatement(query);
        statement.setFetchSize(config.batchSize());
        return statement.executeQuery();
    }

    private Schema createAvroSchema(ResultSetMetaData metaData) throws SQLException {
        final List<Schema.Field> fields = new ArrayList<>();
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            String columnName = metaData.getColumnName(i);
            Schema fieldSchema = getAvroType(metaData.getColumnType(i));
            fields.add(new Schema.Field(columnName, Schema.createUnion(Schema.create(Schema.Type.NULL), fieldSchema), null, Schema.Field.NULL_DEFAULT_VALUE));
        }
        return Schema.createRecord("QueryResult", null, null, false, fields);
    }

    private Schema getAvroType(int sqlType) {
        return switch (sqlType) {
            case java.sql.Types.INTEGER, java.sql.Types.SMALLINT -> Schema.create(Schema.Type.INT);
            case java.sql.Types.BIGINT -> Schema.create(Schema.Type.LONG);
            case java.sql.Types.FLOAT, java.sql.Types.REAL -> Schema.create(Schema.Type.FLOAT);
            case java.sql.Types.DOUBLE, java.sql.Types.NUMERIC, java.sql.Types.DECIMAL ->
                    Schema.create(Schema.Type.DOUBLE);
            case java.sql.Types.BOOLEAN -> Schema.create(Schema.Type.BOOLEAN);
            default -> Schema.create(Schema.Type.STRING);
        };
    }

    private static Config loadConfig(String configFilePath) throws IOException {
        try (final var reader = new FileReader(configFilePath)) {
            final var gson = new Gson();
            return gson.fromJson(reader, Config.class);
        }
    }

    public record Config(String url, String username, String password, int batchSize, String sinceDate) {
    }

    public record SegmentInfo(String segmentName, String tableName) {
    }


}
