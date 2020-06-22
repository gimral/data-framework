package leap.data.beam.serializer;

import leap.data.framework.extension.confluent.kafka.LeapKafkaAvroDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class KafkaSpecificRecordDeserializer<V> implements Deserializer<V> {
    private static final Logger logger = LoggerFactory.getLogger(KafkaSpecificRecordDeserializer.class);
    private final LeapKafkaAvroDeserializer kafkaAvroDeserializer;

    public KafkaSpecificRecordDeserializer() {
        this.kafkaAvroDeserializer = new LeapKafkaAvroDeserializer();
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        //noinspection unchecked
        ((Map<String,Object>)configs).put("specific.avro.reader", "true");
        kafkaAvroDeserializer.configure(configs, isKey);
    }

    @Override
    public V deserialize(String s, byte[] bytes) {
        //noinspection unchecked
        return (V) kafkaAvroDeserializer.deserialize(s, bytes);
    }
}
