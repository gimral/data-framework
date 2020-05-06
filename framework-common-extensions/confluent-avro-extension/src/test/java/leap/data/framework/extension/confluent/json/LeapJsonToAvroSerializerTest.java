package leap.data.framework.extension.confluent.json;

import leap.data.framework.core.serialization.LeapSerializerConfig;
import leap.data.framework.extension.confluent.json.JsonGenericDatumWriter;
import leap.data.framework.extension.confluent.json.LeapAvroToJsonDeserializer;
import leap.data.framework.extension.confluent.json.LeapJsonToAvroSerializer;
import leap.data.framework.extension.confluent.schemaregistry.SchemaRegistryClientFactory;
import leap.data.framework.extension.confluent.schemaregistry.SerializerTestDataProvider;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.generic.GenericData;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class LeapJsonToAvroSerializerTest {
    private LeapJsonToAvroSerializer serializer;
    private LeapAvroToJsonDeserializer deserializer;
    private SchemaRegistryClient client;
    @Before
    public void setUp() {
        GenericData genericData = GenericData.get();
        genericData.addLogicalTypeConversion(new TimeConversions.DateConversion());

        Map<String,Object> config = new HashMap<>();
        config.put("schema.registry.url","mock://");
        serializer = new LeapJsonToAvroSerializer(new LeapSerializerConfig(config));
        deserializer = new LeapAvroToJsonDeserializer(new LeapSerializerConfig(config));
        client = new SchemaRegistryClientFactory().getSchemaRegistryClient(new LeapSerializerConfig(config));
    }

    @Test
    public void shouldserializetoavro() throws IOException, RestClientException {
        Schema schema = SerializerTestDataProvider.AVRO_SCHEMA_EVENT_ACCOUNT_CREATED;
        client.register("AccountCreated",schema);

        byte[] bytes = serializer.serialize("AccountCreated", SerializerTestDataProvider.JSON_DATA_EVENT_ACCOUNT_CREATED);

        Object deserialized = deserializer.deserialize(bytes);
        Assert.assertEquals(SerializerTestDataProvider.JSON_DATA_EVENT_ACCOUNT_CREATED,deserialized.toString());
    }


}
