package knapp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by michael on 7/1/17.
 */
public class Trials {


    private static class MyWriteSupport extends WriteSupport<MyObject> {

        private RecordConsumer recordConsumer;

        public WriteContext init(Configuration configuration) {
            MessageType schema = createMessageType();
            Map<String,String> extraMetaData = new HashMap<String, String>();
            return new WriteContext(schema,extraMetaData);
        }

        public void prepareForWrite(RecordConsumer recordConsumer) {
            this.recordConsumer = recordConsumer;
        }

        public void write(MyObject record) {
            recordConsumer.startMessage();
            recordConsumer.startField("name",0);
            recordConsumer.addBinary(Binary.fromString(record.name));
            recordConsumer.endField("name",0);
            recordConsumer.startField("number",1);
            recordConsumer.addInteger(record.number);
            recordConsumer.endField("number",1);
            recordConsumer.endMessage();
        }
    }

    private static MessageType createMessageType() {
        Type nameType = new PrimitiveType(Type.Repetition.REPEATED,
                PrimitiveType.PrimitiveTypeName.BINARY,"name");
        Type numberType = new PrimitiveType(Type.Repetition.REPEATED,
                PrimitiveType.PrimitiveTypeName.INT32,"number");
        return new MessageType("mymessage",nameType,numberType);
    }


    private static class MyObject {
        String name;
        int number;
    }
}
