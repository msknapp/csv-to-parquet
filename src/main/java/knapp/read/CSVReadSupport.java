package knapp.read;

import knapp.domain.CSVSchema;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

import java.util.Map;

/**
 * Created by michael on 7/2/17.
 */
public class CSVReadSupport extends ReadSupport<String[]> {

    private Configuration configuration;
    private Map<String, String> keyValueMetaData;
    private CSVSchema csvSchema;

    public ReadContext init(
            Configuration configuration,
            Map<String, String> keyValueMetaData,
            MessageType fileSchema) {
        return new ReadContext(fileSchema,keyValueMetaData);
    }

    public CSVReadSupport(CSVSchema csvSchema) {
        this.csvSchema = csvSchema;
    }

    @Override
    public RecordMaterializer<String[]> prepareForRead(Configuration configuration,
                                                       Map<String, String> keyValueMetaData,
                                                       MessageType fileSchema, ReadContext readContext) {
        this.configuration = configuration;

        return new CSVMaterializer(csvSchema);
    }
}
