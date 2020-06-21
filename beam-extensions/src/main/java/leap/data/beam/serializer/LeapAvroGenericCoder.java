package leap.data.beam.serializer;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import leap.data.framework.core.serialization.LeapSerializerConfig;
import leap.data.framework.extension.confluent.schemaregistry.SchemaRegistryClientFactory;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.Coder;

import java.io.IOException;
import java.util.Map;

public class LeapAvroGenericCoder {
    public static Coder<GenericRecord> of(String schemaName, Map<?,?> props) throws IOException, RestClientException {
        SchemaRegistryClient schemaRegistryClient = new SchemaRegistryClientFactory().getSchemaRegistryClient(new LeapSerializerConfig(props));
        Schema schema = new Schema.Parser().parse(schemaRegistryClient.getLatestSchemaMetadata(schemaName).getSchema());
        return AvroCoder.of(GenericRecord.class,schema);
    }
    public static Coder<GenericRecord> of(String schemaStr) {
        Schema schema = new Schema.Parser().parse(schemaStr);
        return AvroCoder.of(GenericRecord.class,schema);
    }
}
