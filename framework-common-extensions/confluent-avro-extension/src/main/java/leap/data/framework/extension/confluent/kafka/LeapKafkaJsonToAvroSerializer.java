package leap.data.framework.extension.confluent.kafka;

import io.confluent.kafka.serializers.AvroSchemaUtils;
import leap.data.framework.core.serialization.LeapSerializerConfig;
import leap.data.framework.extension.confluent.avro.LeapAvroSerializer;
import leap.data.framework.extension.confluent.json.LeapJsonToAvroSerializer;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

@SuppressWarnings("unused")
public class LeapKafkaJsonToAvroSerializer implements Serializer<Object> {
    private boolean isKey;
    private LeapJsonToAvroSerializer avroSerializer;
    //Kafka Client Constructor
    @SuppressWarnings("unused")
    public LeapKafkaJsonToAvroSerializer() {
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        this.isKey = isKey;
        avroSerializer = new LeapJsonToAvroSerializer(new LeapSerializerConfig(configs));
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
