package me.mkhwang.parquetservey.recorddata;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.tools.json.JsonRecordFormatter;
import org.apache.parquet.tools.read.SimpleRecord;
import org.apache.parquet.tools.read.SimpleRecordMaterializer;
import org.json.JSONArray;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Objects;

@Slf4j
public class GreenTripDataTest {

    @Test
    void read__() {
        final String fileName = "green_tripdata_2022-01.parquet";
        final ClassLoader classLoader = getClass().getClassLoader();
        final File file = new File(Objects.requireNonNull(classLoader.getResource(fileName)).getFile());
        try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(
                new Path(file.getAbsolutePath()), new Configuration()))) {
            final MessageType schema = reader.getFooter().getFileMetaData().getSchema();
            final JsonRecordFormatter.JsonGroupFormatter formatter = JsonRecordFormatter.fromSchema(schema);
            JSONArray jsonArray = new JSONArray();
            for (PageReadStore page; (page = reader.readNextRowGroup()) != null; ) {
                long rows = page.getRowCount();
                final MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
                final RecordReader<SimpleRecord> recordReader =
                        columnIO.getRecordReader(page, new SimpleRecordMaterializer(schema));
                for (int i = 0; i < rows; i++) {
                    final SimpleRecord simpleRecord = recordReader.read();
                    final String record = formatter.formatRecord(simpleRecord);
                    final ObjectMapper objectMapper = new ObjectMapper();
                    final String recordPretty = objectMapper.writerWithDefaultPrettyPrinter()
                            .writeValueAsString(objectMapper.readTree(record));
                    jsonArray.put(recordPretty);
                }
            }
            final java.nio.file.Path output = Paths.get("src", "test", "resources", "output.txt");
            Files.write(output, jsonArray.toString().getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
