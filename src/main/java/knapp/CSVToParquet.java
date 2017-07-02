package knapp;

import knapp.data.Row;
import knapp.spec.CSVSchema;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by michael on 7/1/17.
 */
public class CSVToParquet {

    // GOAL: write a parquet file when given CSV.

    // https://github.com/Parquet/parquet-format
    // https://github.com/Parquet/parquet-mr

    // an example:

    // I think I need to implement parquet.hadoop.ParquetWriter
    // https://github.com/Parquet/parquet-mr/blob/master/parquet-hadoop/src/main/java/parquet/hadoop/ParquetWriter.java

    public static void main(String[] args) throws IOException {

        // the input file is a single csv file.
        String inputFile = args[0];

        //
        String inputSchemaFile = args[1];

        // the output base should be a directory we can drop files into.
        String outputBase = args[2];
        String delimiter = args[3];
        int rowsPerOutputFile = Integer.parseInt(args[4]);


        CSVSchema csvSchema = CSVSchema.fromFile(inputSchemaFile);

        String messageTypeName = determineMessageTypeName(inputFile);
        MessageType messageType = csvSchema.deriveMessageType(messageTypeName);

        ParquetFileWriter.Mode mode = ParquetFileWriter.Mode.CREATE;

        FileReader fileReader = new FileReader(new File(inputFile));
        BufferedReader reader = new BufferedReader(fileReader);
        ArrayList<String> lines = new ArrayList<String>(rowsPerOutputFile);
        String line = null;

        CSVToParquet csvToParquet = new CSVToParquet(csvSchema,messageType,delimiter);

        int fileCount = 0;
        while ((line = reader.readLine())!= null) {
            lines.add(line);
            if (lines.size() == rowsPerOutputFile) {
                csvToParquet.createFile(lines,outputBase,fileCount);
                fileCount++;
                lines.clear();
            }
        }
        if (!lines.isEmpty()) {
            csvToParquet.createFile(lines,outputBase,fileCount);
        }
        reader.close();
        fileReader.close();
    }

    private CSVSchema csvSchema;
    private MessageType messageType;
    private String delimiter;

    public CSVToParquet(CSVSchema csvSchema,MessageType messageType,String delimiter) {
        this.csvSchema = csvSchema;
        this.messageType = messageType;
        this.delimiter = delimiter;
    }


    private void createFile(List<String> lines,String outputBase, int fileCount) throws IOException {
        String p = outputBase+"/"+fileCount+".parquet";
        Path path = new Path(p);
        ParquetWriter<String[]> parquetWriter = new ParquetCSVWriterBuilder(path,csvSchema,messageType.getName())
                .enableValidation()
                .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_2_0)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .build();

        if (csvSchema.requiresSort()) {
            int[] sortColumns = csvSchema.deriveSortColumns();
            // convert to rows,
            List<Row> rows = new ArrayList<>(lines.size());
            for (String line : lines) {
                Row row = new Row(sortColumns);
                String[] ps = line.split(delimiter);
                for (int i = 0; i < ps.length; i++) {
                    ps[i] = ps[i].trim();
                }
                row.setCells(ps);
                rows.add(row);
            }
            // sort
            if (csvSchema.requiresSort()) {
                Collections.sort(rows);
            }

//        csvTable = new CSVTable(csvSchema);
            // convert to a table.
            for (Row row : rows) {
                parquetWriter.write(row.getCells());
            }
        } else {
            for (String line : lines) {
                String[] ps = line.split(delimiter);
                for (int i = 0; i < ps.length; i++) {
                    ps[i] = ps[i].trim();
                }
                parquetWriter.write(ps);
            }
        }
        parquetWriter.close();
    }

    private static String determineMessageTypeName(String inputFilePath) {
        String s = inputFilePath.substring(inputFilePath.lastIndexOf("/")+1);
        s = s.substring(0,s.lastIndexOf("."));
        s = s.replaceAll("\\W+","_");
        return s;
    }
}