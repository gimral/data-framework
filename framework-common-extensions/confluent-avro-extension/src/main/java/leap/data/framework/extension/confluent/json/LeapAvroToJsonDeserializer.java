package leap.data.framework.extension.confluent.json;

import leap.data.framework.core.serialization.avro.AvroDeserializer;
import leap.data.framework.core.serialization.json.JsonGenericDatumWriter;
import leap.data.framework.extension.confluent.avro.LeapAvroDeserializer;
import leap.data.framework.core.serialization.LeapSerializerConfig;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import tech.allegro.schema.json2avro.converter.JsonAvroConverter;

public class LeapAvroToJsonDeserializer implements AvroDeserializer<Object> {
    private final AvroDeserializer<Object> avroDeserializer;
    private final JsonAvroConverter jsonAvroConverter;

    public LeapAvroToJsonDeserializer(LeapSerializerConfig config){
        avroDeserializer = new LeapAvroDeserializer(config);
        jsonAvroConverter = new JsonAvroConverter();
    }

    @Override
    public Object deserialize(byte[] payload) {
        Object avroRecord = avroDeserializer.deserialize(payload);
        //JsonGenericDatumWriter writer = new JsonGenericDatumWriter();
        return new String(jsonAvroConverter.convertToJson((GenericRecord) avroRecord));
    }

    @Override
    public Object deserialize(byte[] payload, Schema readerSchema) {
        Object avroRecord = avroDeserializer.deserialize(payload, readerSchema);
        //JsonGenericDatumWriter writer = new JsonGenericDatumWriter();
        return new String(jsonAvroConverter.convertToJson((GenericRecord) avroRecord));
    }
}
