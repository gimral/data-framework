package leap.data.framework.extension.confluent.avro;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import leap.data.framework.core.serialization.avro.AvroDeserializer;
import leap.data.framework.core.serialization.avro.AvroSerializer;
import leap.data.framework.core.serialization.LeapSerializerConfig;
import leap.data.framework.extension.confluent.schemaregistry.SchemaRegistryClientFactory;
import leap.data.framework.extension.confluent.TestDataProvider;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.generic.GenericData;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class LeapAvroSerializerPerformanceTest {
    private AvroSerializer<Object> serializer;
    private AvroDeserializer<Object> deserializer;

    @Before
    public void setUp() throws IOException, RestClientException {
        GenericData genericData = GenericData.get();
        genericData.addLogicalTypeConversion(new TimeConversions.DateConversion());

        Map<String,Object> config = new HashMap<>();
        config.put("schema.registry.url","mock://");
        serializer = new LeapAvroSerializer(new LeapSerializerConfig(config));
        deserializer = new LeapAvroDeserializer(new LeapSerializerConfig(config));
        SchemaRegistryClient client = new SchemaRegistryClientFactory().getSchemaRegistryClient(new LeapSerializerConfig(config));
        client.register("AccountCreated", TestDataProvider.AVRO_SCHEMA_EVENT_ACCOUNT_CREATED);
    }

    @Test
    public void testSerializerPerformance(){
        //when:
        long startTimeMillis = System.currentTimeMillis();
        for(int i = 0; i < 1000000 ; i++) {
            serializer.serialize("", TestDataProvider.GENERIC_RECORD_DATA_EVENT_ACCOUNT_CREATED);
        }
        long endTimeMillis = System.currentTimeMillis();
        long elapsed = endTimeMillis - startTimeMillis;
        System.out.println(elapsed);

        //then:
        assertThat(elapsed)
                .as("Serialization should take less than 3 seconds!")
                .isLessThan(3000);
    }

    @Test
    public void testDeserializerPerformance(){
        //given:
        byte[] bytes = serializer.serialize("", TestDataProvider.GENERIC_RECORD_DATA_EVENT_ACCOUNT_CREATED);

        //then:
        long startTimeMillis = System.currentTimeMillis();
        for(int i = 0; i < 1000000 ; i++) {
            deserializer.deserialize(bytes);
        }
        long endTimeMillis = System.currentTimeMillis();
        long elapsed = endTimeMillis - startTimeMillis;
        System.out.println(elapsed);

        //then:
        assertThat(elapsed)
                .as("Deserialization should take less than 2 seconds!")
                .isLessThan(2000);
    }

}
