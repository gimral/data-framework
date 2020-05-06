package leap.data.framework.extension.confluent.schemaregistry;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaString;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.easymock.EasyMock.*;
import static org.junit.Assert.assertEquals;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class LeapSchemaRegistryClientTest {
    private static final int IDENTITY_MAP_CAPACITY = 5;
    private static final String SUBJECT_0 = "foo";
    private static final int VERSION_1 = 1;
    private static final int ID_25 = 25;

    private static final int RETRY_COUNT = 3;
    private static final long RETRY_SLEEP = 100L;
    private RestService restService;
    private SchemaRegistryClient client;

    @Before
    public void setUp() {
        restService = createNiceMock(RestService.class);
        Map<String,Object> config = new HashMap<>();
        config.put("serializer.registry.retry.maxRetries",RETRY_COUNT);
        config.put("serializer.registry.retry.maxBackoff",RETRY_SLEEP);
        client = new SchemaRegistryClientFactory().getSchemaRegistryClient(restService, IDENTITY_MAP_CAPACITY, config,null);
    }

    @Test(expected = IOException.class)
    public void getByIdShouldThrowExceptionAfterRetries() throws IOException, RestClientException {
        expect(restService.getId(eq(ID_25))).andThrow(new IOException()).times(RETRY_COUNT+1);
        replay(restService);

        client.getById(ID_25);

        verify(restService);
    }

    @Test
    public void getByIdIOExceptionsShouldBeRetried() throws IOException, RestClientException {
        SchemaString schemaString = new SchemaString(SerializerTestDataProvider.SCHEMA_STR_0);
        expect(restService.getId(eq(ID_25))).andThrow(new IOException()).once();
        expect(restService.getId(eq(ID_25))).andReturn(schemaString);
        replay(restService);

        Schema schema = client.getById(ID_25);

        assertEquals(SerializerTestDataProvider.AVRO_SCHEMA_0, schema);
        verify(restService);
    }

    @Test
    public void registerIOExceptionsShouldBeRetried() throws IOException, RestClientException {
        expect(restService.registerSchema(anyString(), eq(SUBJECT_0))).andThrow(new IOException()).once();
        expect(restService.registerSchema(anyString(), eq(SUBJECT_0)))
                .andReturn(ID_25)
                .once();
        replay(restService);

        int id = client.register(SUBJECT_0, SerializerTestDataProvider.AVRO_SCHEMA_0);
        assertEquals(ID_25, id);
        verify(restService);
    }



    @Test
    public void testResource() throws IOException {
        ArrayList<Schema> schemas = new ArrayList<>();
        ClassLoader loader = Thread.currentThread().getContextClassLoader();

        InputStream inputStream = loader.getResourceAsStream("avro/avro.properties");
        InputStreamReader streamReader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
        BufferedReader reader = new BufferedReader(streamReader);
        for (String line; (line = reader.readLine()) != null;) {
            InputStream schemainputStream = loader.getResourceAsStream("avro/" + line);
            Schema schema = new Schema.Parser().parse(schemainputStream);
            schemas.add(schema);
        }

        Assert.assertEquals(1,schemas.size());
    }

//    @Test
//    public void testNewSchema() throws IOException, RestClientException {
//        Map<String,Object> config = new HashMap<>();
//        config.put("schema.registry.url","http://192.168.50.12:31742/registry/");
//        SchemaRegistryClient schemaRegistryClient = new SchemaRegistryClientFactory().getSchemaRegistryClient(new LeapSerializerConfig(config));
//        schemaRegistryClient.register("Test1",SerializerTestDataProvider.AVRO_SCHEMA_0);
//    }

}
