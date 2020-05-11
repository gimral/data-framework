package leap.data.framework.extension.confluent.avro;

import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.serializers.NonRecordContainer;
import leap.data.framework.core.serialization.avro.AvroSerializer;
import leap.data.framework.core.serialization.LeapSerializerConfig;
import leap.data.framework.extension.confluent.schemaregistry.SchemaRegistryClientFactory;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.errors.SerializationException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class LeapAvroSerializer extends AbstractKafkaAvroSerializer implements AvroSerializer<Object> {

    public LeapAvroSerializer(LeapSerializerConfig config){
        configure(config);
    }

    public void configure(LeapSerializerConfig config) {
        schemaRegistry = new SchemaRegistryClientFactory().getSchemaRegistryClient(config);
        super.configure(new KafkaAvroSerializerConfig(config.getProps()));
    }

    @Override
    public byte[] serialize(String subject, Object object) throws SerializationException {
        return serializeImpl(subject, object);
    }

    public String getSubjectName(String topic, boolean isKey, Object value, Schema schema){
        return super.getSubjectName(topic, isKey, value, schema);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public byte[] serializeWithSchemaMetadata(Object object, SchemaMetadata schemaMetadata) throws SerializationException {
        if (object == null) {
            return null;
        } else {
            Schema schema;
            schema = (new Schema.Parser()).parse(schemaMetadata.getSchema());

            try {

                ByteArrayOutputStream out = new ByteArrayOutputStream();
                out.write(0);
                out.write(ByteBuffer.allocate(4).putInt(schemaMetadata.getId()).array());
                if (object instanceof byte[]) {
                    out.write((byte[])object);
                } else {
                    BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(out, null);
                    Object value = object instanceof NonRecordContainer ? ((NonRecordContainer)object).getValue() : object;
                    DatumWriter writer;
                    if (value instanceof SpecificRecord) {
                        writer = new SpecificDatumWriter(schema);
                    } else {
                        writer = new GenericDatumWriter(schema);
                    }

                    writer.write(value, encoder);
                    encoder.flush();
                }

                byte[] bytes = out.toByteArray();
                out.close();
                return bytes;
            } catch (RuntimeException | IOException e) {
                throw new SerializationException("Error serializing Avro message", e);
            }
        }
    }
}
