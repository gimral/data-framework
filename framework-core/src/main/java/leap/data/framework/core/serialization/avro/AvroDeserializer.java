package leap.data.framework.core.serialization.avro;

import org.apache.avro.Schema;

public interface AvroDeserializer<T> {
    T deserialize(byte[] payload);
    T deserialize(byte[] payload, Schema readerSchema);
}
