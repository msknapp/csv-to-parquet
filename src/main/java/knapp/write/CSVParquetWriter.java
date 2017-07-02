package knapp.write;

import knapp.domain.CSVSchema;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;

import java.io.*;

/**
 * Created by michael on 7/1/17.
 */
public class CSVParquetWriter {

    // GOAL: write a parquet file when given CSV.

    // https://github.com/Parquet/parquet-format
    // https://github.com/Parquet/parquet-mr

    // an example:

    // I think I need to implement parquet.hadoop.ParquetWriter
    // https://github.com/Parquet/parquet-mr/blob/master/parquet-hadoop/src/main/java/parquet/hadoop/ParquetWriter.java

    private final CSVSchema csvSchema;
    private final MessageType messageType;
    private String delimiter = ",";
    private String outputBase;
    private int rowsPerOutputFile = 100000;

    public CSVParquetWriter(CSVSchema csvSchema, String messageTypeName) {
        this.csvSchema = csvSchema;
        this.messageType = csvSchema.deriveMessageType(messageTypeName);
    }

    public CSVParquetWriter withDelimiter(String delimiter) {
        this.delimiter = delimiter;
        return this;
    }

    public CSVParquetWriter withOutputBasePath(String outputBase) {
        this.outputBase = outputBase;
        return this;
    }

    public CSVParquetWriter withRowsPerOutputFile(int rows) {
        this.rowsPerOutputFile = rows;
        return this;
    }

    public void exportParquetFiles(File inputFile) throws IOException {
        FileInputStream fileInputStream = new FileInputStream(inputFile);
        exportParquetFiles(fileInputStream);
        fileInputStream.close();
    }

    public void exportParquetFiles(InputStream inputStream) throws IOException {
        InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
        BufferedReader reader = new BufferedReader(inputStreamReader);
        String line = null;

        int fileCount = 0;

        ParquetWriter<String[]> parquetWriter = createParquetWriter(outputBase, csvSchema, messageType, fileCount);

        int linesWritten = 0;
        while ((line = reader.readLine())!= null) {
            String[] ps = line.split(delimiter);
            for (int i = 0; i < ps.length; i++) {
                ps[i] = ps[i].trim();
            }
            parquetWriter.write(ps);
            linesWritten++;
            if (linesWritten == rowsPerOutputFile) {
                parquetWriter.close();
                fileCount++;
                linesWritten = 0;
                parquetWriter = createParquetWriter(outputBase, csvSchema, messageType, fileCount);
            }
        }
        if (linesWritten > 0) {
            parquetWriter.close();
        }
        reader.close();
        inputStreamReader.close();
    }

    private static ParquetWriter<String[]> createParquetWriter(String outputBase, CSVSchema csvSchema,
                                                               MessageType messageType, int fileCount) throws IOException {
        String p = outputBase+"/"+fileCount+".parquet";
        Path path = new Path(p);
        return new ParquetCSVWriterBuilder(path,csvSchema,messageType.getName())
                .enableValidation()
                .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_2_0)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .build();
    }
}