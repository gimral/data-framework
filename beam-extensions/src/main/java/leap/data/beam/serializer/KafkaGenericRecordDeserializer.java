package leap.data.beam.serializer;

import leap.data.framework.extension.confluent.kafka.LeapKafkaAvroDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class KafkaGenericRecordDeserializer implements Deserializer<GenericRecord> {
    private static final Logger logger = LoggerFactory.getLogger(KafkaGenericRecordDeserializer.class);
    private final LeapKafkaAvroDeserializer kafkaAvroDeserializer;
    private Schema readerSchema;

    public KafkaGenericRecordDeserializer() {
        this.kafkaAvroDeserializer = new LeapKafkaAvroDeserializer();
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        //noinspection unchecked
        ((Map<String,Object>)configs).put("specific.avro.reader", "false");
        kafkaAvroDeserializer.configure(configs, isKey);
        if(configs.containsKey("schema.reader")) {
            readerSchema = new Schema.Parser().parse((String) configs.get("schema.reader"));
            logger.debug("Reader Schema will be used. Schema: " + readerSchema.toString());
        }
    }

    @Override
    public GenericRecord deserialize(String s, byte[] bytes) {
        if(readerSchema != null)
            return (GenericRecord) kafkaAvroDeserializer.deserialize(s, bytes, readerSchema);
        return (GenericRecord) kafkaAvroDeserializer.deserialize(s, bytes);
    }
}
