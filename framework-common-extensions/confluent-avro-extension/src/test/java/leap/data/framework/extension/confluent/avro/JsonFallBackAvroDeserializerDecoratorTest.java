package leap.data.framework.extension.confluent.avro;

import leap.data.framework.core.serialization.LeapSerializerConfig;
import leap.data.framework.extension.confluent.schemaregistry.SchemaRegistryClientFactory;
import leap.data.framework.extension.confluent.schemaregistry.SerializerTestDataProvider;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.generic.GenericData;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class JsonFallBackAvroDeserializerDecoratorTest {
    private JsonFallBackAvroDeserializerDecorator deserializer;
    private LeapAvroSerializer serializer;
    private SchemaRegistryClient client;
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
        client = new SchemaRegistryClientFactory().getSchemaRegistryClient(new LeapSerializerConfig(config));
        client.register("AccountCreated", SerializerTestDataProvider.AVRO_SCHEMA_EVENT_ACCOUNT_CREATED);
    }

    @Test
    public void testFallBackToJsonDeserializer(){
        byte[] bytes = SerializerTestDataProvider.JSON_DATA_EVENT_ACCOUNT_CREATED.getBytes();

        Object deserialized = deserializer.deserialize(bytes);

        Assert.assertEquals(1, deserializer.getCurrentSerializerErrorCount());
        Assert.assertEquals(SerializerTestDataProvider.GENERIC_RECORD_DATA_EVENT_ACCOUNT_CREATED.toString(),deserialized.toString());
    }

    @Test
    public void testSerializeWithoutFallBack(){
        byte[] bytes = serializer.serialize("AccountCreated", SerializerTestDataProvider.GENERIC_RECORD_DATA_EVENT_ACCOUNT_CREATED);

        Object deserialized = deserializer.deserialize(bytes);

        Assert.assertEquals(0, deserializer.getCurrentSerializerErrorCount());
        Assert.assertEquals(SerializerTestDataProvider.GENERIC_RECORD_DATA_EVENT_ACCOUNT_CREATED.toString(), deserialized.toString());
    }

    @Test
    public void testFullFallBackAfterMaxTries(){
        byte[] bytes = SerializerTestDataProvider.JSON_DATA_EVENT_ACCOUNT_CREATED.getBytes();

        deserializer.deserialize(bytes);
        deserializer.deserialize(bytes);

        Assert.assertEquals(false, deserializer.isAvroDefaultSerializer());
    }

    @Test
    public void testFallBackDeserializerUsedAfterFullFallBack(){
        byte[] bytes = SerializerTestDataProvider.JSON_DATA_EVENT_ACCOUNT_CREATED.getBytes();
        byte[] avrobytes = serializer.serialize("AccountCreated", SerializerTestDataProvider.GENERIC_RECORD_DATA_EVENT_ACCOUNT_CREATED);

        deserializer.deserialize(bytes);
        deserializer.deserialize(bytes);

        deserializer.deserialize(avrobytes);

        Assert.assertEquals(false, deserializer.isAvroDefaultSerializer());
        Assert.assertEquals(1, deserializer.getCurrentSerializerErrorCount());
    }

    @Test
    public void testFullFallBackFromFullFallBackState(){
        byte[] bytes = SerializerTestDataProvider.JSON_DATA_EVENT_ACCOUNT_CREATED.getBytes();
        byte[] avrobytes = serializer.serialize("AccountCreated", SerializerTestDataProvider.GENERIC_RECORD_DATA_EVENT_ACCOUNT_CREATED);

        deserializer.deserialize(bytes);
        deserializer.deserialize(bytes);

        deserializer.deserialize(avrobytes);
        deserializer.deserialize(avrobytes);

        Assert.assertEquals(true, deserializer.isAvroDefaultSerializer());
        Assert.assertEquals(0, deserializer.getCurrentSerializerErrorCount());
    }
}
