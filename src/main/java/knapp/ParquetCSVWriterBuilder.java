package knapp;

import knapp.data.CSVTable;
import knapp.spec.CSVSchema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.schema.MessageType;

import java.util.List;

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
        return new CSVSegmentWriteSupport(csvSchema,messageTypeName);
    }
}
