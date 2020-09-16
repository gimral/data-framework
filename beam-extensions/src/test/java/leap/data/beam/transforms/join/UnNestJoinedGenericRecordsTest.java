package leap.data.beam.transforms.join;

import leap.data.beam.TestDataProvider;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class UnNestJoinedGenericRecordsTest {
    private static final Logger logger = LoggerFactory.getLogger(OneToOneJoinTest.class);

    @Rule
    public TestPipeline p = TestPipeline.create();
    @Test
    public void testUnNest(){

        TestStream<GenericRecord> accountsStream = TestStream.create(AvroCoder.of(TestDataProvider.AccountSchema))
                .addElements(TestDataProvider.getGenericAccount(1L,1L))
                .advanceWatermarkToInfinity();

        TestStream<GenericRecord> transactionsStream = TestStream.create(AvroCoder.of(TestDataProvider.TransactionDetailSchema))
                .addElements(TestDataProvider.getGenericTransactionDetail(1L, 1L))
                .advanceWatermarkToInfinity();

        List<KV<Long,GenericRecord>> expectedResult = new ArrayList<>();
        expectedResult.add(getJoinedRecord(1L,1L,1L));

        PCollection<KV<Long, GenericRecord>> accounts = p.apply("Create Accounts", accountsStream)
                .apply("Key Accounts", WithKeys.of((SerializableFunction<GenericRecord, Long>) input -> (long)input.get("acid")))
                .setCoder(KvCoder.of(VarLongCoder.of(), AvroCoder.of(TestDataProvider.AccountSchema)));

        PCollection<KV<Long,GenericRecord>> transactions = p.apply("Create Transactions", transactionsStream)
                .apply("Key Transactions",WithKeys.of((SerializableFunction<GenericRecord, Long>) input -> (long)input.get("acid")))
                .setCoder(KvCoder.of(VarLongCoder.of(),AvroCoder.of(TestDataProvider.TransactionDetailSchema)));


        PCollection<KV<Long,GenericRecord>> joinedRecords =
                accounts.apply("Join with Transactions",
                        OneToOneJoin.inner(transactions))
                        .droppedElementsIgnored()
                .apply("UnNest", UnNestJoinedGenericRecords.of());

        PAssert.that(joinedRecords).containsInAnyOrder(expectedResult);

        p.run().waitUntilFinish();
    }

    @Test
    public void testUnNestWithPartialSchema(){

        TestStream<GenericRecord> accountsStream = TestStream.create(AvroCoder.of(TestDataProvider.AccountSchema))
                .addElements(TestDataProvider.getGenericAccount(1L,1L))
                .advanceWatermarkToInfinity();

        TestStream<GenericRecord> transactionsStream = TestStream.create(AvroCoder.of(TestDataProvider.TransactionDetailSchema))
                .addElements(TestDataProvider.getGenericTransactionDetail(1L, 1L))
                .advanceWatermarkToInfinity();

        List<KV<Long,GenericRecord>> expectedResult = new ArrayList<>();
        expectedResult.add(getPartialJoinedRecord(1L,1L,1L));

        PCollection<KV<Long, GenericRecord>> accounts = p.apply("Create Accounts", accountsStream)
                .apply("Key Accounts", WithKeys.of((SerializableFunction<GenericRecord, Long>) input -> (long)input.get("acid")))
                .setCoder(KvCoder.of(VarLongCoder.of(), AvroCoder.of(TestDataProvider.AccountSchema)));

        PCollection<KV<Long,GenericRecord>> transactions = p.apply("Create Transactions", transactionsStream)
                .apply("Key Transactions",WithKeys.of((SerializableFunction<GenericRecord, Long>) input -> (long)input.get("acid")))
                .setCoder(KvCoder.of(VarLongCoder.of(),AvroCoder.of(TestDataProvider.TransactionDetailSchema)));


        PCollection<KV<Long,GenericRecord>> joinedRecords =
                accounts.apply("Join with Transactions",
                        OneToOneJoin.inner(transactions))
                        .droppedElementsIgnored()
                        .apply("UnNest", UnNestJoinedGenericRecords.of(PartialUnNestSchema));

        PAssert.that(joinedRecords).containsInAnyOrder(expectedResult);

        p.run().waitUntilFinish();
    }

    @Test
    public void testUnNestWithPrefixSchema(){

        TestStream<GenericRecord> accountsStream = TestStream.create(AvroCoder.of(TestDataProvider.AccountSchema))
                .addElements(TestDataProvider.getGenericAccount(1L,1L))
                .advanceWatermarkToInfinity();

        TestStream<GenericRecord> transactionsStream = TestStream.create(AvroCoder.of(TestDataProvider.TransactionDetailSchema))
                .addElements(TestDataProvider.getGenericTransactionDetail(1L, 1L))
                .advanceWatermarkToInfinity();

        List<KV<Long,GenericRecord>> expectedResult = new ArrayList<>();
        expectedResult.add(getPrefixedJoinedRecord(1L,1L,1L));

        PCollection<KV<Long, GenericRecord>> accounts = p.apply("Create Accounts", accountsStream)
                .apply("Key Accounts", WithKeys.of((SerializableFunction<GenericRecord, Long>) input -> (long)input.get("acid")))
                .setCoder(KvCoder.of(VarLongCoder.of(), AvroCoder.of(TestDataProvider.AccountSchema)));

        PCollection<KV<Long,GenericRecord>> transactions = p.apply("Create Transactions", transactionsStream)
                .apply("Key Transactions",WithKeys.of((SerializableFunction<GenericRecord, Long>) input -> (long)input.get("acid")))
                .setCoder(KvCoder.of(VarLongCoder.of(),AvroCoder.of(TestDataProvider.TransactionDetailSchema)));


        PCollection<KV<Long,GenericRecord>> joinedRecords =
                accounts.apply("Join with Transactions",
                        OneToOneJoin.inner(transactions))
                        .droppedElementsIgnored()
                        .apply("UnNest", UnNestJoinedGenericRecords.of(PartialUnNestSchema)
                        .withLeftPrefix("LEFT_").withRightPrefix("RIGHT_"));

        PAssert.that(joinedRecords).containsInAnyOrder(expectedResult);

        p.run().waitUntilFinish();
    }

    public static String UnNestSchemaStr = "" +
            "{" +
            "\"type\": \"record\"," +
            "\"name\": \"Account\"," +
            "\"namespace\": \"leap.data.beam.entity\"," +
            "\"fields\":[" +
            "{\"name\":\"acid\",\"type\":[\"null\", \"long\"]}," +
            "{\"name\":\"cust_id\",\"type\":[\"null\", \"long\"]}," +
            "{\"name\":\"account_name\",\"type\":[\"null\", \"string\"]}," +
            "{\"name\":\"balance\",\"type\":[\"null\", \"double\"]}," +
            "{\"name\":\"tran_id\",\"type\":[\"null\", \"long\"]}," +
            "{\"name\":\"tran_type\",\"type\":[\"null\", \"string\"]}," +
            "{\"name\":\"amount\",\"type\":[\"null\", \"double\"]}" +
            "]}";

    public static Schema UnNestSchema = new Schema.Parser().parse(UnNestSchemaStr);

    public static String PartialUnNestSchemaStr = "" +
            "{" +
            "\"type\": \"record\"," +
            "\"name\": \"Account\"," +
            "\"namespace\": \"leap.data.beam.entity\"," +
            "\"fields\":[" +
            "{\"name\":\"acid\",\"type\":[\"null\", \"long\"]}," +
            "{\"name\":\"cust_id\",\"type\":[\"null\", \"long\"]}," +
            "{\"name\":\"balance\",\"type\":[\"null\", \"double\"]}," +
            "{\"name\":\"tran_id\",\"type\":[\"null\", \"long\"]}," +
            "{\"name\":\"amount\",\"type\":[\"null\", \"double\"]}" +
            "]}";

    public static Schema PartialUnNestSchema = new Schema.Parser().parse(PartialUnNestSchemaStr);

    public static String PrefixedUnNestSchemaStr = "" +
            "{" +
            "\"type\": \"record\"," +
            "\"name\": \"Account\"," +
            "\"namespace\": \"leap.data.beam.entity\"," +
            "\"fields\":[" +
            "{\"name\":\"acid\",\"type\":[\"null\", \"long\"]}," +
            "{\"name\":\"LEFT_cust_id\",\"type\":[\"null\", \"long\"]}," +
            "{\"name\":\"LEFT_balance\",\"type\":[\"null\", \"double\"]}," +
            "{\"name\":\"RIGHT_tran_id\",\"type\":[\"null\", \"long\"]}," +
            "{\"name\":\"amount\",\"type\":[\"null\", \"double\"]}" +
            "]}";

    public static Schema PrefixedUnNestSchema = new Schema.Parser().parse(PrefixedUnNestSchemaStr);

    public KV<Long, GenericRecord> getJoinedRecord(long acid, long cust_id, long tran_id){

        GenericRecord record = new GenericRecordBuilder(UnNestSchema)
                .set("acid", acid)
                .set("cust_id", cust_id)
                .set("account_name", "Account " + acid)
                .set("balance", 100D)
                .set("tran_id", tran_id)
                .set("tran_type", "TestType")
                .set("amount", 10D)
                .build();

        return KV.of(acid,record);
    }

    public KV<Long, GenericRecord> getPartialJoinedRecord(long acid, long cust_id, long tran_id){

        GenericRecord record = new GenericRecordBuilder(PartialUnNestSchema)
                .set("acid", acid)
                .set("cust_id", cust_id)
                .set("balance", 100D)
                .set("tran_id", tran_id)
                .set("amount", 10D)
                .build();

        return KV.of(acid,record);
    }

    public KV<Long, GenericRecord> getPrefixedJoinedRecord(long acid, long cust_id, long tran_id){

        GenericRecord record = new GenericRecordBuilder(PrefixedUnNestSchema)
                .set("acid", acid)
                .set("LEFT_cust_id", cust_id)
                .set("LEFT_balance", 100D)
                .set("RIGHT_tran_id", tran_id)
                .set("amount", 10D)
                .build();

        return KV.of(acid,record);
    }
}
