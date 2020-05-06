package leap.data.framework.extension.confluent.json;

import leap.data.framework.core.serialization.LeapSerializerConfig;
import leap.data.framework.extension.confluent.avro.LeapAvroSerializer;
import leap.data.framework.extension.confluent.json.LeapAvroToJsonDeserializer;
import leap.data.framework.extension.confluent.schemaregistry.SerializerTestDataProvider;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.generic.GenericData;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class LeapAvroToJsonDeserializerTest {
    private LeapAvroSerializer serializer;
    private LeapAvroToJsonDeserializer deserializer;

    @Before
    public void setUp() {
        GenericData genericData = GenericData.get();
        genericData.addLogicalTypeConversion(new TimeConversions.DateConversion());

        Map<String,Object> config = new HashMap<>();
        config.put("schema.registry.url","mock://");
        serializer = new LeapAvroSerializer(new LeapSerializerConfig(config));
        deserializer = new LeapAvroToJsonDeserializer(new LeapSerializerConfig(config));

    }
    //TODO:should test with date logical type
    @Test
    public void testDeserializeToJson(){
        byte[] bytes = serializer.serialize("AccountCreated", SerializerTestDataProvider.GENERIC_RECORD_DATA_EVENT_ACCOUNT_CREATED);

        Object deserialized = deserializer.deserialize(bytes);

        Assert.assertEquals(SerializerTestDataProvider.JSON_DATA_EVENT_ACCOUNT_CREATED,deserialized.toString());
    }
}
