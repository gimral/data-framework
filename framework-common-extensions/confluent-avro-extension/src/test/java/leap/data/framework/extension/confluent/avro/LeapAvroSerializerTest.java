package leap.data.framework.extension.confluent.avro;

import leap.data.framework.core.serialization.AvroDeserializer;
import leap.data.framework.core.serialization.AvroSerializer;
import leap.data.framework.core.serialization.LeapSerializerConfig;
import leap.data.framework.extension.confluent.schemaregistry.SchemaRegistryClientFactory;
import leap.data.framework.extension.confluent.schemaregistry.SerializerTestDataProvider;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.generic.GenericData;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

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

    @Parameterized.Parameters
    public static Collection<Object[]> primitiveValues(){
        return Arrays.asList(new Object[][] {
                {null},
                {1},
                {1L},
                {1.1f},
                {1.1d},
                {""},
                {"abc"},
                {"abc".getBytes()}
        });
    }


    @Test
    @Parameters
    public void testSerializePrimitives(Object primitive){
        byte[] bytes = serializer.serialize("", primitive);
        assertEquals(primitive, deserializer.deserialize(bytes));
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
        byte[] bytes = serializer.serialize("", "abc".getBytes());
        assertArrayEquals("abc".getBytes(), (byte[]) deserializer.deserialize(bytes));
    }

    @Test
    public void testSerializeGenericRecord(){
        byte[] bytes = serializer.serialize("", SerializerTestDataProvider.GENERIC_RECORD_DATA_EVENT_ACCOUNT_CREATED);
        assertEquals(SerializerTestDataProvider.GENERIC_RECORD_DATA_EVENT_ACCOUNT_CREATED, deserializer.deserialize(bytes));
    }
}
