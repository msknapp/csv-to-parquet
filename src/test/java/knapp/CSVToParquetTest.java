package knapp;

import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.Util;
import org.junit.Test;

import java.io.*;
import java.util.Base64;
import java.util.Random;

/**
 * Created by michael on 7/1/17.
 */
public class CSVToParquetTest {

    @Test
    public void testIt() throws IOException {
        CSVToParquet.main(new String[] {
                "/home/michael/workspace/csv-to-parquet/example-input/my.csv",
                "/home/michael/workspace/csv-to-parquet/example-input/my-schema.txt",
                "/home/michael/workspace/csv-to-parquet/tmp",
                ",",
                "5"
        });
    }

    @Test
    public void generateTestData() throws IOException {
        File out = new File("out.csv");
        FileWriter fileWriter = new FileWriter(out);
        Random random = new Random();

        boolean first = true;
        for (int i = 0;i<1000000;i++) {
            if (!first) {
                fileWriter.write('\n');
            }
            first = false;
            byte[] bs = new byte[16];
            random.nextBytes(bs);
            String s = Base64.getEncoder().encodeToString(bs);
            long x = random.nextLong();
            int j = random.nextInt(1000000);
            boolean b = random.nextBoolean();
            double d = random.nextDouble();
            String row = String.format("%d,%s,%d,%d,%b,%f",i,s,x,j,b,d);
            fileWriter.write(row);
        }
        fileWriter.close();
    }
    @Test
    public void playground() throws IOException {
        CSVToParquet.main(new String[] {
                "/home/michael/workspace/csv-to-parquet/playground/input/random-values.csv",
                "/home/michael/workspace/csv-to-parquet/playground/input/random-values-schema.csv",
                "/home/michael/workspace/csv-to-parquet/playground/output",
                ",",
                "100000"
        });
    }

    @Test
    public void check() throws IOException {
        FileInputStream from = new FileInputStream(
                new File("/home/michael/workspace/csv-to-parquet/playground/output/1.parquet"));
        FileMetaData x = Util.readFileMetaData(from);
        System.out.print(x);
        from.close();
    }
}
