package leap.data.framework.extension.confluent.kafka;

import leap.data.framework.extension.confluent.json.LeapJsonToAvroSerializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * A schema-registry aware serializer for writing data in "generic Avro" format.
 *
 * <p>This serializer writes data in the wire format defined at
 * http://docs.confluent.io/current/schema-registry/docs/serializer-formatter.html#wire-format.
 * It requires access to a Confluent Schema Registry endpoint, which you must
 * {@link JsonToGenericAvroSerializer#configure(Map, boolean)} via the parameter
 * "schema.registry.url".</p>
 *
 * <p>See {@link GenericAvroDeserializer} for its deserializer counterpart.</p>
 */
@InterfaceStability.Unstable
public class JsonToGenericAvroSerializer implements Serializer<GenericRecord> {

    private final LeapKafkaJsonToAvroSerializer inner;

    public JsonToGenericAvroSerializer() {
        inner = new LeapKafkaJsonToAvroSerializer();
    }

    @Override
    public void configure(final Map<String, ?> serializerConfig,
                          final boolean isSerializerForRecordKeys) {
        inner.configure(serializerConfig, isSerializerForRecordKeys);
    }

    @Override
    public byte[] serialize(final String topic, final GenericRecord record) {
        return inner.serialize(topic, record);
    }

    @Override
    public void close() {
        inner.close();
    }

}
