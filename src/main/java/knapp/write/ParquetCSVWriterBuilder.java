package knapp.write;

import knapp.domain.CSVSchema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;

/**
 * Created by michael on 7/1/17.
 */
class ParquetCSVWriterBuilder extends ParquetWriter.Builder<String[], ParquetCSVWriterBuilder> {

    private CSVSchema csvSchema;
    private String messageTypeName;

    protected ParquetCSVWriterBuilder(Path file,CSVSchema csvSchema,String messageTypeName) {
        super(file);
        this.csvSchema = csvSchema;
        this.messageTypeName = messageTypeName;
    }

    protected ParquetCSVWriterBuilder self() {
        return this;
    }

    protected WriteSupport<String[]> getWriteSupport(Configuration conf) {
        return new CSVWriteSupport(csvSchema,messageTypeName);
    }
}
