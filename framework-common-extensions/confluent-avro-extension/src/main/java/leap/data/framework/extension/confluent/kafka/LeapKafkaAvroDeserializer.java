package leap.data.framework.extension.confluent.kafka;

import leap.data.framework.core.serialization.AvroDeserializer;
import leap.data.framework.core.serialization.LeapSerializerConfig;
import leap.data.framework.extension.confluent.avro.SerializerFactory;
import org.apache.avro.Schema;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

@SuppressWarnings("unused")
public class LeapKafkaAvroDeserializer implements Deserializer<Object> {
    private AvroDeserializer<Object> avroDeserializer;

    /**
     * Constructor used by Kafka consumer.
     */
    @SuppressWarnings("unused")
    public LeapKafkaAvroDeserializer() {

    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        //configure(new LeapSerializerConfig(configs));
        avroDeserializer = new SerializerFactory().getLeapAvroDeserializer(new LeapSerializerConfig(configs));
    }

    @Override
    public Object deserialize(String s, byte[] bytes) {
        return avroDeserializer.deserialize(bytes);
    }

    /**
     * Pass a reader schema to get an Avro projection
     */
    public Object deserialize(String s, byte[] bytes, Schema readerSchema) {
        return avroDeserializer.deserialize(bytes, readerSchema);
    }

    @Override
    public void close() {

    }
}
