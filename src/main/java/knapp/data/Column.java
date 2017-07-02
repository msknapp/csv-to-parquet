package knapp.data;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.Funnels;
import org.apache.parquet.schema.PrimitiveType;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by michael on 7/1/17.
 */
public class Column {
    private List<String> values = new ArrayList<>();
    private PrimitiveType.PrimitiveTypeName primitiveTypeName;
    private String name;

    public Column(PrimitiveType.PrimitiveTypeName primitiveTypeName) {
        this.primitiveTypeName = primitiveTypeName;
    }

    public void addValue(String x) {
        this.values.add(x);
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<String> getValues() {
        return values;
    }

    public void setValues(List<String> values) {
        this.values = values;
    }

    public Range getRangeOfValues() {
        if (primitiveTypeName == PrimitiveType.PrimitiveTypeName.BINARY) {
            return _getRangeOfValues_String();
        } else if (primitiveTypeName == PrimitiveType.PrimitiveTypeName.INT32 ||
                primitiveTypeName == PrimitiveType.PrimitiveTypeName.INT64 ||
                primitiveTypeName == PrimitiveType.PrimitiveTypeName.INT96) {
            return _getRangeOfValues_Integer();
        } else if (primitiveTypeName == PrimitiveType.PrimitiveTypeName.FLOAT ||
                primitiveTypeName == PrimitiveType.PrimitiveTypeName.DOUBLE) {
            return _getRangeOfValues_Double();
        }
        return null;
    }

    private Range _getRangeOfValues_String() {
        String min = null;
        String max = null;
        for (String s : values) {
            if (min == null || s.compareTo(min) < 0) {
                min = s;
            }
            if (max == null || s.compareTo(max) > 0) {
                max = s;
            }
        }
        return new Range(min,max);
    }

    private Range _getRangeOfValues_Double() {
        double min = Double.MAX_VALUE;
        double max = Double.MIN_VALUE;
        for (String s : values) {
            if (s == null || s.isEmpty()) {
                continue;
            }
            double value = Double.valueOf(s);
            if (value < min) {
                min = value;
            }
            if (value > max) {
                max = value;
            }
        }
        return new Range(min,max);
    }

    private Range _getRangeOfValues_Integer() {
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;
        for (String s : values) {
            if (s == null || s.isEmpty()) {
                continue;
            }
            long value = Long.valueOf(s);
            if (value < min) {
                min = value;
            }
            if (value > max) {
                max = value;
            }
        }
        return new Range(min,max);
    }

    public byte[] createBloomFilter() {
        Funnel funnel = Funnels.stringFunnel(Charset.defaultCharset());
        BloomFilter<String> bloomFilter = BloomFilter.create(funnel,Math.min(10000,values.size()));
        for (String value : values) {
            bloomFilter.put(value);
        }
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        try {
            bloomFilter.writeTo(byteArrayOutputStream);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                byteArrayOutputStream.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return byteArrayOutputStream.toByteArray();
    }


}
