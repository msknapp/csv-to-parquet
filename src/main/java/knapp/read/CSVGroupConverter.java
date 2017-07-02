package knapp.read;

import knapp.domain.CSVColumn;
import knapp.domain.CSVSchema;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by michael on 7/2/17.
 */
public class CSVGroupConverter extends GroupConverter {

    private CSVSchema csvSchema;
    private Map<Integer,CSVColumnConverter> converters;

    public CSVGroupConverter(CSVSchema csvSchema){
        this.csvSchema = csvSchema;
        converters = new HashMap<>(csvSchema.getColumns().size());
        for (CSVColumn csvColumn : csvSchema.getColumns()) {
            converters.put(csvColumn.getNumber(),new CSVColumnConverter(this, csvColumn));
        }
    }

    private String[] currentValue;

    @Override
    public Converter getConverter(int fieldIndex) {
        return converters.get(fieldIndex);
    }

    @Override
    public void start() {
        currentValue = new String[converters.size()];
    }

    public String[] getCurrentValue() {
        return currentValue;
    }

    @Override
    public void end() {

    }

}
