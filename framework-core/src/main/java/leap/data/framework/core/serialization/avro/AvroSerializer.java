package leap.data.framework.core.serialization.avro;

public interface AvroSerializer<T> {
    byte[] serialize(String subject, T object);
}
