package knapp;

import knapp.data.CSVTable;
import knapp.data.Column;
import knapp.data.Range;
import knapp.spec.CSVColumn;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;

import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by michael on 7/1/17.
 */
public class CSVSegmentWriteSupport extends WriteSupport<CSVTable> {

    private RecordConsumer recordConsumer;
    private CSVTable csvTable;
    private String messageTypeName;

    public CSVSegmentWriteSupport(CSVTable csvTable,String messageTypeName) {
        this.csvTable = csvTable;
        this.messageTypeName = messageTypeName;
    }

    public WriteContext init(Configuration configuration) {
        MessageType messageType = csvTable.getCsvSchema().deriveMessageType(messageTypeName);
        Map<String,String> metadata = deriveMetadata(csvTable);
        return new WriteContext(messageType,metadata);
    }

    private Map<String,String> deriveMetadata(CSVTable csvTable) {
        Map<String,String> metadata = new HashMap<>();
        // TODO record vital metadata.
        for (CSVColumn csvColumn : csvTable.getColumns().keySet()) {
            Column column = csvTable.getColumn(csvColumn);
            if (csvColumn.isKeepRange()) {
                // we want to record the range of values.
                Range range = column.getRangeOfValues();
                if (range != null) {
                    metadata.put(csvColumn.getName()+".min",range.getMin());
                    metadata.put(csvColumn.getName()+".max",range.getMax());
                }
            }
            if (csvColumn.isBloomFilter()) {
                byte[] bloomFilter = column.createBloomFilter();
                String bf = Base64.getEncoder().encodeToString(bloomFilter);
                metadata.put(csvColumn.getName()+".bloom-filter",bf);
            }
        }
        String[] sortColumns = csvTable.deriveSortColumns();
        metadata.put("sort-columns", StringUtils.join(sortColumns,","));

        return metadata;
    }

    public void prepareForWrite(RecordConsumer recordConsumer) {
        this.recordConsumer = recordConsumer;
    }

    public void write(CSVTable csvTable) {
        recordConsumer.startMessage();
        for (Map.Entry<CSVColumn,Column> entry : csvTable.getColumns().entrySet()) {
            CSVColumn csvColumn = entry.getKey();
            String fieldName = csvColumn.getName();
            int columnNumber = csvColumn.getNumber();
            recordConsumer.startField(fieldName,columnNumber);
            Column column = entry.getValue();

            PrimitiveType.PrimitiveTypeName type = PrimitiveType.PrimitiveTypeName.valueOf(csvColumn.getType());

            for (String value : column.getValues()) {
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
            }
            recordConsumer.endField(entry.getKey().getName(),entry.getKey().getNumber());
        }
        recordConsumer.endMessage();
    }
}
