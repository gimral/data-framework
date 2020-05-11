package leap.data.framework.core.serialization.avro;

public interface AvroKafkaSerializer<T> extends AvroSerializer<T> {
    byte[] serialize(String subject, Object object);
}
