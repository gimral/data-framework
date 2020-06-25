package leap.data.framework.extension.confluent.kafka;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/**
 * A schema-registry aware deserializer for reading data in "generic Avro" format.
 *
 * <p>This deserializer assumes that the serialized data was written in the wire format defined at
 * http://docs.confluent.io/current/schema-registry/docs/serializer-formatter.html#wire-format.
 * It requires access to a Confluent Schema Registry endpoint, which you must
 * {@link GenericAvroDeserializer#configure(Map, boolean)} via the parameter
 * "schema.registry.url".</p>
 *
 * <p>See {@link GenericAvroSerializer} for its serializer counterpart.</p>
 */
public class GenericAvroDeserializer implements Deserializer<GenericRecord> {

    private final LeapKafkaAvroDeserializer inner;

    public GenericAvroDeserializer() {
        inner = new LeapKafkaAvroDeserializer();
    }

    @Override
    public void configure(final Map<String, ?> deserializerConfig,
                          final boolean isDeserializerForRecordKeys) {
        inner.configure(deserializerConfig, isDeserializerForRecordKeys);
    }

    @Override
    public GenericRecord deserialize(final String topic, final byte[] bytes) {
        return (GenericRecord) inner.deserialize(topic, bytes);
    }

    @Override
    public void close() {
        inner.close();
    }

}
