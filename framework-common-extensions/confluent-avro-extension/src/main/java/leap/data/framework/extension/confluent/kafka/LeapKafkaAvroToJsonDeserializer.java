package leap.data.framework.extension.confluent.kafka;

import leap.data.framework.core.serialization.LeapSerializerConfig;
import leap.data.framework.core.serialization.avro.AvroDeserializer;
import leap.data.framework.extension.confluent.avro.SerializerFactory;
import leap.data.framework.extension.confluent.json.LeapAvroToJsonDeserializer;
import org.apache.avro.Schema;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

@SuppressWarnings("unused")
public class LeapKafkaAvroToJsonDeserializer implements Deserializer<Object> {
    private LeapAvroToJsonDeserializer avroDeserializer;

    /**
     * Constructor used by Kafka consumer.
     */
    @SuppressWarnings("unused")
    public LeapKafkaAvroToJsonDeserializer() {

    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        //configure(new LeapSerializerConfig(configs));
        avroDeserializer = new LeapAvroToJsonDeserializer(new LeapSerializerConfig(configs));
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
