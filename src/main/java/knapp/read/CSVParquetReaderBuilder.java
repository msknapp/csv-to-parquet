package knapp.read;

import knapp.domain.CSVSchema;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.api.ReadSupport;

/**
 * Created by michael on 7/2/17.
 */
public class CSVParquetReaderBuilder extends ParquetReader.Builder<String[]> {

    private CSVSchema csvSchema;

    protected CSVParquetReaderBuilder(Path path, CSVSchema csvSchema) {
        super(path);
        this.csvSchema = csvSchema;
    }

    protected ReadSupport<String[]> getReadSupport() {
        return new CSVReadSupport(csvSchema);
    }

}
