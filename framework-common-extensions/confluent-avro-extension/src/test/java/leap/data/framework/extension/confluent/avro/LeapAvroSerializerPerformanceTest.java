package leap.data.framework.extension.confluent.avro;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import leap.data.framework.core.serialization.AvroDeserializer;
import leap.data.framework.core.serialization.AvroSerializer;
import leap.data.framework.core.serialization.LeapSerializerConfig;
import leap.data.framework.extension.confluent.schemaregistry.SchemaRegistryClientFactory;
import leap.data.framework.extension.confluent.schemaregistry.SerializerTestDataProvider;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class LeapAvroSerializerPerformanceTest {
    private AvroSerializer<Object> serializer;
    private AvroDeserializer<Object> deserializer;
    private SchemaRegistryClient client;

    @Before
    public void setUp() throws IOException, RestClientException {
        GenericData genericData = GenericData.get();
        genericData.addLogicalTypeConversion(new TimeConversions.DateConversion());

        Map<String,Object> config = new HashMap<>();
        config.put("schema.registry.url","mock://");
        serializer = new LeapAvroSerializer(new LeapSerializerConfig(config));
        deserializer = new LeapAvroDeserializer(new LeapSerializerConfig(config));
        client = new SchemaRegistryClientFactory().getSchemaRegistryClient(new LeapSerializerConfig(config));
        client.register("AccountCreated", SerializerTestDataProvider.AVRO_SCHEMA_EVENT_ACCOUNT_CREATED);
    }

    @Test
    public void testSerializerPerformance(){
        long startTimeMillis = System.currentTimeMillis();
        for(int i = 0; i < 1000000 ; i++) {
            serializer.serialize("", SerializerTestDataProvider.GENERIC_RECORD_DATA_EVENT_ACCOUNT_CREATED);
        }
        long endTimeMillis = System.currentTimeMillis();
        long elapsed = endTimeMillis - startTimeMillis;
        System.out.println(elapsed);
        Assert.assertTrue("Serialization took more than 2 seconds",elapsed <= 2000);
    }

    @Test
    public void testDeserializerPerformance(){
        byte[] bytes = serializer.serialize("", SerializerTestDataProvider.GENERIC_RECORD_DATA_EVENT_ACCOUNT_CREATED);
        long startTimeMillis = System.currentTimeMillis();
        for(int i = 0; i < 1000000 ; i++) {
            deserializer.deserialize(bytes);
        }
        long endTimeMillis = System.currentTimeMillis();
        long elapsed = endTimeMillis - startTimeMillis;
        System.out.println(elapsed);
        Assert.assertTrue("Deserialization took more than 2 seconds",elapsed <= 2000);
    }

}
