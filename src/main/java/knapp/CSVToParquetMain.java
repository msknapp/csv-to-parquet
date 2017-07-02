package knapp;

import knapp.domain.CSVSchema;
import knapp.read.CSVParquetReader;
import knapp.write.CSVParquetWriter;
import org.apache.commons.lang.StringUtils;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

/**
 * Created by michael on 7/2/17.
 */
public class CSVToParquetMain {


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

        CSVParquetWriter csvParquetWriter = new CSVParquetWriter(csvSchema,messageTypeName)
                .withDelimiter(delimiter).withOutputBasePath(outputBase)
                .withRowsPerOutputFile(rowsPerOutputFile);

        csvParquetWriter.exportParquetFiles(new File(inputFile));

        for (File file : new File(outputBase).listFiles()) {
            Iterator<String[]> contents = new CSVParquetReader(csvSchema).read(file);
            while (contents.hasNext()) {
                String[] x = contents.next();
                String t = StringUtils.join(x,",");
                System.out.println(t);
            }
        }
    }

    private static String determineMessageTypeName(String inputFilePath) {
        String s = inputFilePath.substring(inputFilePath.lastIndexOf("/")+1);
        s = s.substring(0,s.lastIndexOf("."));
        s = s.replaceAll("\\W+","_");
        return s;
    }
}
