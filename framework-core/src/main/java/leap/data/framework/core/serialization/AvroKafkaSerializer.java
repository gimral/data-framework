package leap.data.framework.core.serialization;

public interface AvroKafkaSerializer<T> extends AvroSerializer<T> {
    byte[] serialize(String subject, Object object);
}
