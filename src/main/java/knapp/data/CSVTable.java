package knapp.data;

import knapp.spec.CSVColumn;
import knapp.spec.CSVSchema;
import org.apache.parquet.schema.PrimitiveType;

import java.util.*;

/**
 * Created by michael on 7/1/17.
 */
public class CSVTable {

    private Map<CSVColumn,Column> columns = new HashMap<>();
    private CSVSchema csvSchema;

    public CSVTable(CSVSchema csvSchema) {
        this.csvSchema = csvSchema;
        for (CSVColumn col : csvSchema.getColumns()) {
            Column x = new Column(PrimitiveType.PrimitiveTypeName.valueOf(col.getType()));
            x.setName(col.getName());
            columns.put(col,x);
        }
    }

    public CSVSchema getCsvSchema() {
        return csvSchema;
    }

    public Map<CSVColumn,Column> getColumns() {


        return Collections.unmodifiableMap(columns);
    }

    public Column getColumn(CSVColumn csvColumn) {
        return columns.get(csvColumn);
    }

    public String[] deriveSortColumns() {
        int count = 0;
        for (CSVColumn col : csvSchema.getColumns()) {
            if (col.getSort() > 0) {
                count++;
            }
        }
        if (count < 1) {
            return new String[0];
        }
        String[] out = new String[count];
        for (CSVColumn col : csvSchema.getColumns()) {
            if (col.getSort() > 0) {
                out[col.getSort()-1] = col.getName();
            }
        }
        return out;
    }
}
