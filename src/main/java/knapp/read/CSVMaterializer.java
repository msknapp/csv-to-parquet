package knapp.read;

import knapp.domain.CSVSchema;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.RecordMaterializer;

/**
 * Created by michael on 7/2/17.
 */
public class CSVMaterializer extends RecordMaterializer<String[]> {

    private CSVSchema csvSchema;
    private CSVGroupConverter root;

    public CSVMaterializer(CSVSchema csvSchema) {
        this.csvSchema = csvSchema;
        this.root = new CSVGroupConverter(csvSchema);
    }

    @Override
    public String[] getCurrentRecord() {
        return root.getCurrentValue();
    }

    @Override
    public GroupConverter getRootConverter() {
        return root;
    }
}
