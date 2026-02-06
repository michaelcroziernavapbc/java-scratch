package gov.nj.dol;

import com.google.gson.Gson;

import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.avro.Schema;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.List;

public class DB2Downloader {

    private final Config config;

    public static void main(String[] args) throws Exception {
        final var config = loadConfig(args[0]);
        new DB2Downloader(config).downloadQueryResults();
    }

    public DB2Downloader(Config config) {
        this.config = config;
        System.out.println(config);
    }

    public void downloadQueryResults() throws SQLException, IOException {
        final var connection = getDB2Connection(config.url(), config.username(), config.password());

        try (final var resultSet = executeQuery(connection, config.query())) {
            ResultSetMetaData metaData = resultSet.getMetaData();
            Schema avroSchema = createAvroSchema(metaData);

            //Path outputPath = new Path("output.parquet");
            final var outputFile = new LocalOutputFile(Path.of(config.outputPath));
            Configuration conf = new Configuration();

            try (ParquetWriter<GenericRecord> writer = AvroParquetWriter
                    .<GenericRecord>builder(outputFile)
                    .withSchema(avroSchema)
                    .withConf(conf)
                    .withCompressionCodec(CompressionCodecName.SNAPPY)
                    .withRowGroupRowCountLimit(config.batchSize())
                    .build()) {

                while (resultSet.next()) {
                    GenericRecord record = new GenericData.Record(avroSchema);
                    for (int i = 1; i <= metaData.getColumnCount(); i++) {
                        record.put(metaData.getColumnName(i), resultSet.getObject(i));
                    }
                    writer.write(record);
                }
            }
        }
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
        PreparedStatement statement = connection.prepareStatement(query);
        statement.setFetchSize(config.batchSize());
        return statement.executeQuery();
    }

    private Schema createAvroSchema(ResultSetMetaData metaData) throws SQLException {
        List<Schema.Field> fields = new ArrayList<>();
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

    public record Config(String url, String username, String password, String query, String outputPath, int batchSize) {
    }
}
