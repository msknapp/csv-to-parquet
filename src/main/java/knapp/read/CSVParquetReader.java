package knapp.read;

import knapp.domain.CSVSchema;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

/**
 * Created by michael on 7/2/17.
 */
public class CSVParquetReader {

    private CSVSchema csvSchema;

    public CSVParquetReader(CSVSchema csvSchema) {
        this.csvSchema = csvSchema;
    }

    public Iterator<String[]> read(File file) throws IOException {
        ParquetReader<String[]> simpleParquetReader = new CSVParquetReaderBuilder(
                new Path(file.getAbsolutePath()),csvSchema)
                .build();
        return new SimpleParquetIterator(simpleParquetReader);
    }

    public static final class SimpleParquetIterator implements Iterator<String[]> {

        private String[] theNextRow;
        private ParquetReader<String[]> simpleParquetReader;

        public SimpleParquetIterator(ParquetReader<String[]> simpleParquetReader) {
            this.simpleParquetReader = simpleParquetReader;
            findNext();
        }

        private void findNext() {
            theNextRow = null;
            try {
                theNextRow = simpleParquetReader.read();
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                if (theNextRow == null) {
                    try {
                        simpleParquetReader.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }

        @Override
        public boolean hasNext() {
            return theNextRow != null;
        }

        @Override
        public String[] next() {
            String[] current = theNextRow;
            findNext();
            return current;
        }
    }
}