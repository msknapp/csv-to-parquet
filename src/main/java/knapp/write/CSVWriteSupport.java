package knapp.write;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.Funnels;
import knapp.domain.CSVColumn;
import knapp.domain.CSVSchema;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by michael on 7/1/17.
 */
public class CSVWriteSupport extends WriteSupport<String[]> {

    private RecordConsumer recordConsumer;
    private CSVSchema csvSchema;
    private String messageTypeName;
    private Map<CSVColumn,BloomFilter<CharSequence>> bloomFilters = new HashMap<>();
    private Map<CSVColumn,WorkingRange> ranges = new HashMap<>();

    // lessons learned:
    // 1. you can edit metadata during the run of writes, so you don't need all data up front.
    // 2. the write method must correspond to a single row.  parquet is counting this for you and adding that to metadata.

    public CSVWriteSupport(CSVSchema csvSchema, String messageTypeName,int expectedValues) {
        this.csvSchema = csvSchema;
        this.messageTypeName = messageTypeName;
        for (CSVColumn column : csvSchema.getColumns()) {
            if (column.isBloomFilter()) {
                Funnel<CharSequence> funnel = Funnels.stringFunnel(Charset.defaultCharset());
                BloomFilter<CharSequence> bloomFilter = BloomFilter.create(funnel,expectedValues);
                bloomFilters.put(column,bloomFilter);
            }
            if (column.isKeepRange()) {
                ranges.put(column,new WorkingRange(PrimitiveType.PrimitiveTypeName.valueOf(column.getType())));
            }
        }
    }

    public WriteContext init(Configuration configuration) {
        MessageType messageType = csvSchema.deriveMessageType(messageTypeName);
        Map<String,String> metadata = new HashMap<>();
        return new WriteContext(messageType,metadata);
    }

    public FinalizedWriteContext finalizeWrite() {
        Map<String,String> meta = new HashMap<String, String>();
        for (CSVColumn csvColumn : bloomFilters.keySet()) {
            BloomFilter<CharSequence> bloomFilter = bloomFilters.get(csvColumn);
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            try {
                bloomFilter.writeTo(byteArrayOutputStream);
            } catch (IOException e) {
                e.printStackTrace();
            }
            byte[] bytes = byteArrayOutputStream.toByteArray();
            String bf = Base64.getEncoder().encodeToString(bytes);
            meta.put(csvColumn.getName()+".bloom",bf);
        }
        for (CSVColumn csvColumn : ranges.keySet()) {
            WorkingRange range = ranges.get(csvColumn);
            meta.put(csvColumn.getName()+".min",range.getMin());
            meta.put(csvColumn.getName()+".max",range.getMax());
        }
        for (CSVColumn csvColumn : csvSchema.getColumns()) {
            if (csvColumn.getSortOrder() != CSVColumn.SortOrder.UNSORTED) {
                meta.put(csvColumn.getName()+".sort",String.valueOf(csvColumn.getSortNumber()));
                meta.put(csvColumn.getName()+".sort-direction",csvColumn.getSortOrder().name());
            }
        }

        return new FinalizedWriteContext(meta);
    }

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

            if (csvColumn.isKeepRange()) {
                ranges.get(csvColumn).consider(value);
            }
            if (csvColumn.isBloomFilter()) {
                BloomFilter<CharSequence> bloomFilter = bloomFilters.get(csvColumn);
                bloomFilter.put(value);
            }
        }
        recordConsumer.endMessage();
    }

    private static class WorkingRange {
        private String min,max;
        private double minD = Double.MAX_VALUE;
        private double maxD = Double.MIN_VALUE;
        private long minL = Long.MAX_VALUE;
        private long maxL = Long.MIN_VALUE;

        private final PrimitiveType.PrimitiveTypeName primitiveTypeName;
        public WorkingRange(PrimitiveType.PrimitiveTypeName primitiveTypeName) {
            this.primitiveTypeName = primitiveTypeName;
        }
        public String getMin() {
            if (isDecimal()) {
                return String.valueOf(minD);
            } else if (isInteger()) {
                return String.valueOf(minL);
            }
            return min;
        }
        public String getMax() {
            if (isDecimal()) {
                return String.valueOf(maxD);
            } else if (isInteger()) {
                return String.valueOf(maxL);
            }
            return max;
        }

        public void consider(String value) {
            if (isDecimal()) {
                double d = Double.valueOf(value);
                if (d < minD) {
                    minD = d;
                }
                if (d > maxD) {
                    maxD = d;
                }
            } else if (isInteger()) {
                long d = Long.valueOf(value);
                if (d < minL) {
                    minL = d;
                }
                if (d > maxL) {
                    maxL = d;
                }
            } else {
                if (min == null || value.compareTo(min) < 0) {
                    min = value;
                }
                if (max == null || value.compareTo(max) > 0) {
                    max = value;
                }
            }
        }

        public boolean isInteger() {
            return primitiveTypeName == PrimitiveType.PrimitiveTypeName.INT32 ||
                    primitiveTypeName == PrimitiveType.PrimitiveTypeName.INT64 ||
                    primitiveTypeName == PrimitiveType.PrimitiveTypeName.INT96;
        }

        public boolean isDecimal() {
            return primitiveTypeName == PrimitiveType.PrimitiveTypeName.DOUBLE ||
                    primitiveTypeName == PrimitiveType.PrimitiveTypeName.FLOAT;
        }
    }
}