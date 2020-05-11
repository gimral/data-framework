package leap.data.framework.extension.confluent.json;

import leap.data.framework.core.serialization.LeapSerializerConfig;
import leap.data.framework.extension.confluent.avro.LeapAvroSerializer;
import leap.data.framework.extension.confluent.TestDataProvider;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.generic.GenericData;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

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

    @Test
    public void testDeserializeToJson(){
        //given:
        byte[] bytes = serializer.serialize("AccountCreated", TestDataProvider.GENERIC_RECORD_DATA_EVENT_ACCOUNT_CREATED);

        //when:
        Object deserialized = deserializer.deserialize(bytes);

        //then:
        assertThat(deserialized.toString())
                .isEqualTo(TestDataProvider.JSON_DATA_EVENT_ACCOUNT_CREATED);
    }
}
