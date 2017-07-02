package knapp.spec;

import org.apache.commons.io.FileUtils;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by michael on 7/1/17.
 */
public class CSVSchema {
    private List<CSVColumn> columns = new ArrayList<>();

    public static CSVSchema fromText(String text) {
        CSVSchema csvSchema = new CSVSchema();
        int i = 0;
        for (String line : text.split("\n")) {
            if (line.trim().startsWith("#")) {
                continue;
            }
            if (line.isEmpty()) {
                continue;
            }
            CSVColumn csvColumn = CSVColumn.fromLine(line,i);
            csvSchema.columns.add(csvColumn);
            i++;
        }
        return csvSchema;
    }

    public boolean requiresSort() {
        for (CSVColumn column : columns) {
            if (column.getSort() > 0) {
                return true;
            }
        }
        return false;
    }

    public static CSVSchema fromFile(String filePath) throws IOException {
        String text = FileUtils.readFileToString(new File(filePath));
        return fromText(text);
    }

    public MessageType deriveMessageType(String name) {
        Type[] types = new Type[this.columns.size()];
        int i = 0;
        for (CSVColumn column : this.columns) {
            types[i++] = new PrimitiveType(Type.Repetition.REPEATED,
                    PrimitiveType.PrimitiveTypeName.valueOf(column.getType()),column.getName());
        }
        return new MessageType(name,types);
    }

    public CSVSchema() {

    }

    public int[] deriveSortColumns() {
        int total = 0;
        for (int i = 0;i<columns.size();i++) {
            if (columns.get(i).getSort() > 0) {
                total++;
            }
        }
        if (total < 1) {
            return new int[0];
        }
        int[] out = new int[total];
        for (int i = 0;i<columns.size();i++) {
            CSVColumn col = columns.get(i);
            if (col.getSort() > 0) {
                out[col.getSort()-1] = col.getNumber();
            }
        }
        return out;
    }

    public List<CSVColumn> getColumns() {
        return columns;
    }

    public void setColumns(List<CSVColumn> columns) {
        this.columns = columns;
    }
}
