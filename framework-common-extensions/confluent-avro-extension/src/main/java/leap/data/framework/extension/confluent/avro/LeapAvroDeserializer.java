package leap.data.framework.extension.confluent.avro;

import io.confluent.kafka.serializers.AbstractKafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import leap.data.framework.core.serialization.AvroDeserializer;
import leap.data.framework.core.serialization.LeapSerializerConfig;
import leap.data.framework.extension.confluent.schemaregistry.SchemaRegistryClientFactory;
import org.apache.avro.Schema;
import org.apache.kafka.common.errors.SerializationException;

public class LeapAvroDeserializer extends AbstractKafkaAvroDeserializer implements AvroDeserializer<Object> {

    public LeapAvroDeserializer(LeapSerializerConfig configs){
        schemaRegistry = new SchemaRegistryClientFactory().getSchemaRegistryClient(configs);
        configure(new KafkaAvroDeserializerConfig(configs.getProps()));
    }

    @Override
    public Object deserialize(byte[] payload) throws SerializationException {
        return super.deserialize(payload);
    }

    @Override
    public Object deserialize(byte[] payload, Schema readerSchema) throws SerializationException {
        return super.deserialize(payload, readerSchema);
    }
}
