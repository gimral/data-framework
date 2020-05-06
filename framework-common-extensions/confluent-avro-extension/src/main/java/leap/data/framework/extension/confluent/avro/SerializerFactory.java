package leap.data.framework.extension.confluent.avro;

import leap.data.framework.core.serialization.AvroDeserializer;
import leap.data.framework.core.serialization.LeapSerializerConfig;

public class SerializerFactory {

    public AvroDeserializer<Object> getLeapAvroDeserializer(LeapSerializerConfig config){
        AvroDeserializer<Object> deserializer = new LeapAvroDeserializer(config);
        if(JsonFallBackAvroDeserializerDecorator.isEnabled(config))
            deserializer = new JsonFallBackAvroDeserializerDecorator(deserializer, config);
        return deserializer;
    }
}

