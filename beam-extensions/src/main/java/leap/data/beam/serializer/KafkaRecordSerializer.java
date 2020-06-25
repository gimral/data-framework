package leap.data.beam.serializer;

import leap.data.framework.extension.confluent.kafka.LeapKafkaAvroDeserializer;
import leap.data.framework.extension.confluent.kafka.LeapKafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class KafkaRecordSerializer<V> implements Serializer<V> {
    private static final Logger logger = LoggerFactory.getLogger(KafkaRecordSerializer.class);
    private final LeapKafkaAvroSerializer kafkaAvroSerializer;

    public KafkaRecordSerializer() {
        this.kafkaAvroSerializer = new LeapKafkaAvroSerializer();
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        //noinspection unchecked
        ((Map<String,Object>)configs).put("specific.avro.reader", "false");
        kafkaAvroSerializer.configure(configs, isKey);
    }

    @Override
    public byte[] serialize(String s, V v) {
        return kafkaAvroSerializer.serialize(s, v);
    }
}
