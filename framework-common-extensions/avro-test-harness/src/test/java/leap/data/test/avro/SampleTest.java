package leap.data.test.avro;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.confluent.common.utils.IntegrationTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({IntegrationTest.class})
public class SampleTest {
    private static ConfluentTestHarness confluentTestHarness;

    @BeforeClass
    public static void setUp() throws Exception {
        //System.setProperty("zookeeper.sasl.client.canonicalize.hostname", "false");
        confluentTestHarness = new ConfluentTestHarness();
        confluentTestHarness.setUp();
        System.getenv().put("bootstrapServers", confluentTestHarness.getBootstrapServers());
        System.getenv().put("schema.registry.url", confluentTestHarness.getSchemaRegistryUrl());
    }

    private Account getAccount(){
        Account account = new Account();
        account.setAcid(1L);
        account.setAccountType("AcType");
        account.setBalance(100D);
        account.setBranchCode(1000L);
        return account;
    }

    @Test
    public void testProduceJson() throws JsonProcessingException {

        //given:
        Account account = getAccount();
        SampleJsonApp sampleJsonApp =  new SampleJsonApp();
        //when:
        sampleJsonApp.produceAccountToKafka(account);
        //then:
        //TODO: Consume from kafka
    }

    @Test
    public void testProduceAvro() throws JsonProcessingException {
        //given:
        Account account = getAccount();
        SampleAvroApp sampleAvroApp =  new SampleAvroApp();
        //when:
        sampleAvroApp.produceAccountToKafka(account);
        //then:
        //TODO: Consume from kafka
    }


    @AfterClass
    public static void tearDown() throws Exception {
        confluentTestHarness.tearDown();
    }



}
