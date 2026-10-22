package gov.nj.dol;

import com.google.gson.Gson;

import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
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
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/*
SELECT * FROM OTDUIMP.UIMT_SG01_CLAIMANT WHERE SG01_DTE_CLAIM_UIB >= (current date - 5 YEAR)
SELECT * FROM OTDUIMP.UIMT_SG02_PAYMENT WHERE SG02_DTE_CLAIM_UIB >= (current date - 5 YEAR)
SELECT * FROM OTDUIMP.UIMT_SG03_EMPLOYER WHERE SG03_DTE_CLAIM_UIB >= (current date - 5 YEAR)
SELECT * FROM OTDUIMP.UIMT_SG05_EXTENSION WHERE SG05_DTE_CLAIM_UIB >= (current date - 5 YEAR)
SELECT * FROM OTDUIMP.UIMT_SG07_MONETARY_HISTORY WHERE SG07_DTE_CLAIM_UIB >= (current date - 5 YEAR)
SELECT * FROM OTDUIMP.UIMT_SG10_REAS_ADDL WHERE SG10_DTE_CLAIM_UIB >= (current date - 5 YEAR)
SELECT * FROM OTDUIMP.UIMT_SG11_NONMON WHERE SG11_DTE_CLAIM_UIB >= (current date - 5 YEAR)
SELECT * FROM OTDUIMP.UIMT_SG15_DEPENDENCY WHERE SG15_DTE_CLAIM_UIB >= (current date - 5 YEAR)
SELECT * FROM OTDUIMP.UIMT_SG16_REMARKS WHERE SG16_DTE_CLAIM_UIB >= (current date - 5 YEAR)
SELECT * FROM OTDUIMP.UIMT_SG04_EMP_CHARGE WHERE SG04_DTE_CLAIM_UIB >= (current date - 5 YEAR)
SELECT * FROM OTDUIMP.UIMT_SG17_26B_FORM WHERE SG17_DTE_CLAIM_UIB >= (current date - 5 YEAR)
SELECT * FROM OTDUIMP.UIMT_SG18_26B_ROUTING WHERE SG18_DTE_CLAIM_UIB >= (current date - 5 YEAR)
SELECT * FROM OTDUIMP.UIMT_SG19_EMP_289D WHERE SG19_DTE_CLAIM_UIB >= (current date - 5 YEAR)

20m
 */

public class DB2Downloader {

    private final Config config;
    private final List<SegmentInfo> segmentsAndQueries = List.of(
            new SegmentInfo("SG01", "UIMT_SG01_CLAIMANT", DateFilter.DTE_CLAIM_UIB),
            new SegmentInfo("SG02", "UIMT_SG02_PAYMENT", DateFilter.DTE_CLAIM_UIB),
            new SegmentInfo("SG03", "UIMT_SG03_EMPLOYER", DateFilter.DTE_CLAIM_UIB),
            new SegmentInfo("SG04", "UIMT_SG04_EMP_CHARGE", DateFilter.DTE_CLAIM_UIB),
            new SegmentInfo("SG05", "UIMT_SG05_EXTENSION", DateFilter.NONE),
            new SegmentInfo("SG07", "UIMT_SG07_MONETARY_HISTORY", DateFilter.DTE_CLAIM_UIB),
            new SegmentInfo("SG10", "UIMT_SG10_REAS_ADDL", DateFilter.DTE_CLAIM_UIB),
            new SegmentInfo("SG11", "UIMT_SG11_NONMON", DateFilter.DTE_CLAIM_UIB),
            new SegmentInfo("SG15", "UIMT_SG15_DEPENDENCY", DateFilter.DTE_CLAIM_UIB),
            new SegmentInfo("SG16", "UIMT_SG16_REMARKS", DateFilter.DTE_CLAIM_UIB),
            new SegmentInfo("SG17", "UIMT_SG17_26B_FORM", DateFilter.DTE_CLAIM_UIB),
            new SegmentInfo("SG18", "UIMT_SG18_26B_ROUTING", DateFilter.DTE_CLAIM_UIB),
            new SegmentInfo("SG19", "UIMT_SG19_EMP_289D", DateFilter.DTE_CLAIM_UIB),
            new SegmentInfo("SG08", "UIMT_SG08_REFUND", DateFilter.DATE_CLAIM),
            new SegmentInfo("SG09", "UIMT_SG09_REFUND_PAYMENT", DateFilter.DATE_CLAIM),
            new SegmentInfo("SG14", "UIMT_SG14_COD", DateFilter.DATE_CLAIM)
    );

    private final LocalDate today = LocalDate.now();
    private final String dateToday = today.toString();

    public static void main(String[] args) throws Exception {
        final var config = loadConfig(args[0]);
        var db2Downloader = new DB2Downloader(config);

        db2Downloader.downloadAllSegments();
    }

    private void downloadAllSegments() throws SQLException, IOException {
        final var connection = getDB2Connection(config.url(), config.username(), config.password());
        for (var segmentInfo : selectedSegments()) {
            long startTime = System.nanoTime();

            String query = formatQueryForSegment(segmentInfo);
            String outputPath = formatOutputPathForSegment(segmentInfo);
            Path path = Path.of(outputPath).getParent();
            if (path != null) {
                Files.createDirectories(path);
            }
            downloadQueryResults(connection, query, outputPath);

            long endTime = System.nanoTime();
            System.out.println("Time taken for segment " + segmentInfo.segmentName + ": " + (endTime - startTime) / 1_000_000 + " ms");
        }
    }

    // No "segments" in the config means all of them.
    private List<SegmentInfo> selectedSegments() {
        if (config.segments() == null || config.segments().isEmpty()) {
            return segmentsAndQueries;
        }
        final var known = segmentsAndQueries.stream().map(SegmentInfo::segmentName).toList();
        final var unknown = config.segments().stream().filter(s -> !known.contains(s)).toList();
        if (!unknown.isEmpty()) {
            throw new IllegalArgumentException("Unknown segments " + unknown + "; expected some of " + known);
        }
        return segmentsAndQueries.stream().filter(s -> config.segments().contains(s.segmentName())).toList();
    }

    private String formatOutputPathForSegment(SegmentInfo segmentInfo) {
        return "output/%s/%s-%s.parquet".formatted(dateToday, segmentInfo.segmentName, dateToday);
    }

    private String formatQueryForSegment(SegmentInfo segmentInfo) {
        // A literal date rather than (current date - N MONTH); the DB2 team prefers it.
        final var sinceDate = today.minusMonths(config.monthsBack());
        return switch (segmentInfo.dateFilter) {
            case DTE_CLAIM_UIB -> "SELECT * FROM OTDUIMP.%s WHERE %s_DTE_CLAIM_UIB >= '%s' WITH UR".formatted(segmentInfo.tableName, segmentInfo.segmentName, sinceDate);
            case DATE_CLAIM -> "SELECT * FROM OTDUIMP.%s WHERE %s_DATE_CLAIM >= '%s' WITH UR".formatted(segmentInfo.tableName, segmentInfo.segmentName, sinceDate);
            case NONE -> "SELECT * FROM OTDUIMP.%s WITH UR".formatted(segmentInfo.tableName);
        };
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

    public record Config(String url, String username, String password, int batchSize, int monthsBack, List<String> segments) {
    }

    // Which claim-date column limits the download; NONE pulls the whole table every time.
    public enum DateFilter { DTE_CLAIM_UIB, DATE_CLAIM, NONE }

    public record SegmentInfo(String segmentName, String tableName, DateFilter dateFilter) {
    }
}