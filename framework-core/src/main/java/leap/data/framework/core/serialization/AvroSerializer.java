package leap.data.framework.core.serialization;

public interface AvroSerializer<T> {
    byte[] serialize(String subject, T object);
}
