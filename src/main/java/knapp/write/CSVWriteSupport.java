package knapp.write;

import knapp.domain.CSVColumn;
import knapp.domain.CSVSchema;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by michael on 7/1/17.
 */
public class CSVWriteSupport extends WriteSupport<String[]> {

    private RecordConsumer recordConsumer;
    private CSVSchema csvSchema;
    private String messageTypeName;

    public CSVWriteSupport(CSVSchema csvSchema, String messageTypeName) {
        this.csvSchema = csvSchema;
        this.messageTypeName = messageTypeName;
    }

    public WriteContext init(Configuration configuration) {
        MessageType messageType = csvSchema.deriveMessageType(messageTypeName);
        Map<String,String> metadata = new HashMap<>();
        return new WriteContext(messageType,metadata);
    }

    // lessons learned:
    // 1. you can edit metadata during the run of writes, so you don't need all data up front.
    // 2. the write method must correspond to a single row.  parquet is counting this for you and adding that to metadata.
    //



//    private Map<String,String> deriveMetadata(CSVTable csvTable) {
//        Map<String,String> metadata = new HashMap<>();
//        for (CSVColumn csvColumn : csvTable.getColumns().keySet()) {
//            Column column = csvTable.getColumn(csvColumn);
//            if (csvColumn.isKeepRange()) {
//                // we want to record the range of values.
//                Range range = column.getRangeOfValues();
//                if (range != null) {
//                    metadata.put(csvColumn.getName()+".min",range.getMin());
//                    metadata.put(csvColumn.getName()+".max",range.getMax());
//                }
//            }
//            if (csvColumn.isBloomFilter()) {
//                byte[] bloomFilter = column.createBloomFilter();
//                String bf = Base64.getEncoder().encodeToString(bloomFilter);
//                metadata.put(csvColumn.getName()+".bloom-filter",bf);
//            }
//        }
//        String[] sortColumns = csvTable.deriveSortColumns();
//        metadata.put("sort-columns", StringUtils.join(sortColumns,","));
//
//        return metadata;
//    }

    public void prepareForWrite(RecordConsumer recordConsumer) {
        this.recordConsumer = recordConsumer;
    }

    public void write(String[] values) {
        recordConsumer.startMessage();
        for (CSVColumn csvColumn : csvSchema.getColumns()) {
            String fieldName = csvColumn.getName();
            int columnNumber = csvColumn.getNumber();
            recordConsumer.startField(fieldName,columnNumber);
            String value = values[csvColumn.getNumber()];

            PrimitiveType.PrimitiveTypeName type = PrimitiveType.PrimitiveTypeName.valueOf(csvColumn.getType());

            if (type == PrimitiveType.PrimitiveTypeName.INT96 ||
                    type == PrimitiveType.PrimitiveTypeName.INT64) {
                recordConsumer.addLong(Long.valueOf(value));
            } else if (type == PrimitiveType.PrimitiveTypeName.INT32) {
                recordConsumer.addInteger(Integer.valueOf(value));
            } else if (type == PrimitiveType.PrimitiveTypeName.FLOAT) {
                recordConsumer.addFloat(Float.valueOf(value));
            } else if (type == PrimitiveType.PrimitiveTypeName.DOUBLE) {
                recordConsumer.addDouble(Double.valueOf(value));
            } else if (type == PrimitiveType.PrimitiveTypeName.BOOLEAN) {
                recordConsumer.addBoolean(Boolean.valueOf(value));
            } else {
                recordConsumer.addBinary(Binary.fromString(value));
            }
            recordConsumer.endField(csvColumn.getName(),csvColumn.getNumber());

            // TODO update metadata about that column
            // min values, max values, bloom filters, etc.
        }
        recordConsumer.endMessage();
    }
}
