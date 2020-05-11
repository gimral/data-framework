package leap.data.framework.extension.confluent.avro;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
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

public class JsonFallBackAvroDeserializerDecoratorTest {
    private JsonFallBackAvroDeserializerDecorator deserializer;
    private LeapAvroSerializer serializer;

    @Before
    public void setUp() throws IOException, RestClientException {
        GenericData genericData = GenericData.get();
        genericData.addLogicalTypeConversion(new TimeConversions.DateConversion());

        Map<String,Object> config = new HashMap<>();
        config.put("schema.registry.url","mock://");
        config.put("serializer.avro.fallback.json.maxFullFallBackTry",1);
        config.put("serializer.avro.fallback.json.schema.name","AccountCreated");
        deserializer = (JsonFallBackAvroDeserializerDecorator)new SerializerFactory().getLeapAvroDeserializer(new LeapSerializerConfig(config));
        serializer = new LeapAvroSerializer(new LeapSerializerConfig(config));
        SchemaRegistryClient client = new SchemaRegistryClientFactory().getSchemaRegistryClient(new LeapSerializerConfig(config));
        client.register("AccountCreated", TestDataProvider.AVRO_SCHEMA_EVENT_ACCOUNT_CREATED);
    }

    @Test
    public void testFallBackToJsonDeserializer(){
        //given:
        byte[] bytes = TestDataProvider.JSON_DATA_EVENT_ACCOUNT_CREATED.getBytes();

        //when:
        Object deserialized = deserializer.deserialize(bytes);

        //then:
        assertThat(deserializer.getCurrentSerializerErrorCount())
                .as("Deserializer error count is wrong!")
                .isEqualTo(1);

        assertThat(TestDataProvider.GENERIC_RECORD_DATA_EVENT_ACCOUNT_CREATED.toString())
                .as("Deserialized data is wrong!")
                .isEqualTo(deserialized.toString());
    }

    @Test
    public void testSerializeWithoutFallBack(){
        //given:
        byte[] bytes = serializer.serialize("AccountCreated", TestDataProvider.GENERIC_RECORD_DATA_EVENT_ACCOUNT_CREATED);
        //when:
        Object deserialized = deserializer.deserialize(bytes);
        //then:
        assertThat(deserializer.getCurrentSerializerErrorCount())
                .as("No error should occur!")
                .isEqualTo(0);

        assertThat(TestDataProvider.GENERIC_RECORD_DATA_EVENT_ACCOUNT_CREATED.toString())
                .as("Deserialized data is wrong!")
                .isEqualTo(deserialized.toString());
    }

    @Test
    public void testFullFallBackAfterMaxTries(){
        //given:
        byte[] bytes = TestDataProvider.JSON_DATA_EVENT_ACCOUNT_CREATED.getBytes();

        //when:
        deserializer.deserialize(bytes);
        deserializer.deserialize(bytes);

        //then:
        assertThat(deserializer.isAvroDefaultSerializer())
                .as("Json  should be default serializer!")
                .isFalse();
    }

    @Test
    public void testFallBackDeserializerUsedAfterFullFallBack(){
        //given:
        byte[] bytes = TestDataProvider.JSON_DATA_EVENT_ACCOUNT_CREATED.getBytes();
        byte[] avrobytes = serializer.serialize("AccountCreated", TestDataProvider.GENERIC_RECORD_DATA_EVENT_ACCOUNT_CREATED);

        //when:
        deserializer.deserialize(bytes);
        deserializer.deserialize(bytes);
        deserializer.deserialize(avrobytes);

        //then:
        assertThat(deserializer.isAvroDefaultSerializer())
                .as("Json  should be default serializer!")
                .isFalse();

        assertThat(deserializer.getCurrentSerializerErrorCount())
                .as("Deserializer error count is wrong!")
                .isEqualTo(1);
    }

    @Test
    public void testFullFallBackFromFullFallBackState(){
        //given:
        byte[] bytes = TestDataProvider.JSON_DATA_EVENT_ACCOUNT_CREATED.getBytes();
        byte[] avrobytes = serializer.serialize("AccountCreated", TestDataProvider.GENERIC_RECORD_DATA_EVENT_ACCOUNT_CREATED);

        //then:
        deserializer.deserialize(bytes);
        deserializer.deserialize(bytes);

        deserializer.deserialize(avrobytes);
        deserializer.deserialize(avrobytes);

        //then:
        assertThat(deserializer.isAvroDefaultSerializer())
                .as("Json  should be default serializer!")
                .isTrue();

        assertThat(deserializer.getCurrentSerializerErrorCount())
                .as("Error count should be cleared!")
                .isEqualTo(0);
    }
}
