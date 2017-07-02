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
class ParquetCSVWriterBuilder extends ParquetWriter.Builder<CSVTable, ParquetCSVWriterBuilder> {

    private CSVTable csvTable;
    private String messageTypeName;

    protected ParquetCSVWriterBuilder(Path file,CSVTable csvTable,String messageTypeName) {
        super(file);
        this.csvTable = csvTable;
        this.messageTypeName = messageTypeName;
    }

    protected ParquetCSVWriterBuilder self() {
        return this;
    }

    protected WriteSupport getWriteSupport(Configuration conf) {
        return new CSVSegmentWriteSupport(csvTable,messageTypeName);
    }
}
