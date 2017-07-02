package knapp;

import knapp.data.CSVTable;
import knapp.data.Column;
import knapp.data.Row;
import knapp.spec.CSVColumn;
import knapp.spec.CSVSchema;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.schema.MessageType;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
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
        CSVTable csvTable = null;
        int[] sortColumns = csvSchema.deriveSortColumns();
        // convert to rows,
        List<Row> rows = new ArrayList<>(lines.size());
        for (String line : lines) {
            Row row = new Row(sortColumns);
            String[] ps = line.split(delimiter);
            for (int i = 0;i<ps.length;i++) {
                ps[i] = ps[i].trim();
            }
            row.setCells(ps);
            rows.add(row);
        }
        // sort
        if (csvSchema.requiresSort()) {
            Collections.sort(rows);
        }

        csvTable = new CSVTable(csvSchema);
        // convert to a table.
        for (Row row : rows) {
            for (CSVColumn column : csvSchema.getColumns()) {
                Column col = csvTable.getColumn(column);
                String val = row.getCells()[column.getNumber()];
                col.addValue(val);
            }
        }
        String p = outputBase+"/"+fileCount+".parquet";
        Path path = new Path(p);
        ParquetWriter<CSVTable> parquetWriter = new ParquetCSVWriterBuilder(path,csvTable,messageType.getName()).build();
        parquetWriter.write(csvTable);
        parquetWriter.close();
    }

    private static String determineMessageTypeName(String inputFilePath) {
        String s = inputFilePath.substring(inputFilePath.lastIndexOf("/")+1);
        s = s.substring(0,s.lastIndexOf("."));
        s = s.replaceAll("\\W+","_");
        return s;
    }
}