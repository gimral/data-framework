package leap.data.framework.extension.confluent.kafka;

import io.confluent.kafka.serializers.AvroSchemaUtils;
import leap.data.framework.extension.confluent.avro.LeapAvroSerializer;
import leap.data.framework.core.serialization.LeapSerializerConfig;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

@SuppressWarnings("unused")
public class LeapKafkaAvroSerializer implements Serializer<Object> {
    private boolean isKey;
    private LeapAvroSerializer avroSerializer;
    //Kafka Client Constructor
    @SuppressWarnings("unused")
    public LeapKafkaAvroSerializer() {
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        this.isKey = isKey;
        avroSerializer = new LeapAvroSerializer(new LeapSerializerConfig(configs));
    }

    @Override
    public byte[] serialize(String topic, Object record) {
        return avroSerializer.serialize(
                avroSerializer.getSubjectName(topic, isKey, record, AvroSchemaUtils.getSchema(record)), record);
    }

    @Override
    public void close() {

    }

}
