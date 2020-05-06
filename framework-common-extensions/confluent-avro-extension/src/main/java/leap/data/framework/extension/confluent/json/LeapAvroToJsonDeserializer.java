package leap.data.framework.extension.confluent.json;

import leap.data.framework.core.serialization.AvroDeserializer;
import leap.data.framework.extension.confluent.avro.LeapAvroDeserializer;
import leap.data.framework.core.serialization.LeapSerializerConfig;
import org.apache.avro.Schema;

public class LeapAvroToJsonDeserializer implements AvroDeserializer<Object> {
    private final AvroDeserializer<Object> avroDeserializer;

    public LeapAvroToJsonDeserializer(LeapSerializerConfig config){
        avroDeserializer = new LeapAvroDeserializer(config);
    }

    @Override
    public Object deserialize(byte[] payload) {
        Object avroRecord = avroDeserializer.deserialize(payload);
        JsonGenericDatumWriter writer = new JsonGenericDatumWriter();
        return writer.write(avroRecord);
    }

    @Override
    public Object deserialize(byte[] payload, Schema readerSchema) {
        Object avroRecord = avroDeserializer.deserialize(payload, readerSchema);
        JsonGenericDatumWriter writer = new JsonGenericDatumWriter();
        return writer.write(avroRecord);
    }
}
