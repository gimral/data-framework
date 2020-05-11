package leap.data.framework.extension.confluent.avro;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import leap.data.framework.core.serialization.LeapSerializerConfig;
import leap.data.framework.core.serialization.avro.AvroDeserializer;
import leap.data.framework.core.serialization.avro.AvroSerializer;
import leap.data.framework.extension.confluent.TestDataProvider;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.generic.GenericData;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(JUnitParamsRunner.class)
public class LeapAvroSerializerTest {
    private AvroSerializer<Object> serializer;
    private AvroDeserializer<Object> deserializer;

    @Before
    public void setUp() {
        GenericData genericData = GenericData.get();
        genericData.addLogicalTypeConversion(new TimeConversions.DateConversion());

        Map<String,Object> config = new HashMap<>();
        config.put("schema.registry.url","mock://");
        serializer = new LeapAvroSerializer(new LeapSerializerConfig(config));
        deserializer = new LeapAvroDeserializer(new LeapSerializerConfig(config));
    }


    @Test
    @Parameters
    public void testSerializePrimitives(Object primitive){
        //when:
        byte[] bytes = serializer.serialize("", primitive);
        //then:
        assertThat(deserializer.deserialize(bytes))
                .isEqualTo(primitive);
    }

    //Used by Test Frameworks
    @SuppressWarnings("unused")
    private Object parametersForTestSerializePrimitives(){
        return new Object[][] {
                {null},
                {1},
                {1L},
                {1.1f},
                {1.1d},
                {""},
                {"abc"}
        };
    }

    @Test
    public void testSerializeBytes(){
        //when:
        byte[] bytes = serializer.serialize("", "abc".getBytes());
        //then:
        assertThat(deserializer.deserialize(bytes))
                .isEqualTo("abc".getBytes());
    }

    @Test
    public void testSerializeGenericRecord(){
        //when:
        byte[] bytes = serializer.serialize("", TestDataProvider.GENERIC_RECORD_DATA_EVENT_ACCOUNT_CREATED);
        //then:
        assertThat(deserializer.deserialize(bytes))
                .isEqualTo(TestDataProvider.GENERIC_RECORD_DATA_EVENT_ACCOUNT_CREATED);
    }
}
