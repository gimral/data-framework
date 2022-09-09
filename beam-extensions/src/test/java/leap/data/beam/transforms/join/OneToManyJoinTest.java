package leap.data.beam.transforms.join;

import leap.data.beam.TestDataProvider;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class OneToManyJoinTest {
    //TODO:Test state is emptied
    private static final Logger logger = LoggerFactory.getLogger(OneToManyJoinTest.class);

    @Rule
    public TestPipeline p = TestPipeline.create();

    public static DoFn<KV<Long, KV<GenericRecord, GenericRecord>>, KV<Long, KV<GenericRecord, GenericRecord>>> getReKeyFn() {
        return new DoFn<KV<Long, KV<GenericRecord, GenericRecord>>, KV<Long, KV<GenericRecord, GenericRecord>>>() {
            @ProcessElement
            public void processElement(@Element KV<Long, KV<GenericRecord, GenericRecord>> element, ProcessContext context) {
                if (element.getValue().getValue() == null)
                    context.output(KV.of(null, element.getValue()));
                else
                    context.output(KV.of((Long) element.getValue().getValue().get("tran_id"), element.getValue()));
            }
        };
    }

    public static DoFn<KV<Long, KV<GenericRecord, GenericRecord>>, String> getGenericJoinLogger() {
        return new DoFn<KV<Long, KV<GenericRecord, GenericRecord>>, String>() {
            @ProcessElement
            public void processElement(@Element KV<Long, KV<GenericRecord, GenericRecord>> element) {
                if (element.getValue() == null)
                    logger.error("Joined Generic {}-{}-{}", element.getKey(),
                            null, null);
                else
                    logger.error("Joined Generic {}-{}-{}", element.getKey(),
                            element.getValue().getKey(), element.getValue().getValue());
            }
        };
    }

    public static DoFn<KV<Long, KV<GenericRecord, KV<GenericRecord, GenericRecord>>>, String> getMultiGenericJoinLogger() {
        return new DoFn<KV<Long, KV<GenericRecord, KV<GenericRecord, GenericRecord>>>, String>() {
            @ProcessElement
            public void processElement(@Element KV<Long, KV<GenericRecord, KV<GenericRecord, GenericRecord>>> element) {
                if (element.getValue() == null)
                    logger.error("Joined Generic Multi {}-{}-{}-{}", element.getKey(),
                            null, null, null);
                else if (element.getValue().getValue() == null)
                    logger.error("Joined Generic Multi {}-{}-{}-{}", element.getKey(),
                            element.getValue().getKey(), null, null);
                else
                    logger.error("Joined Generic Multi {}-{}-{}-{}", element.getKey(),
                            element.getValue().getKey(), element.getValue().getValue().getKey(),
                            element.getValue().getValue().getValue());
            }
        };
    }

    @Test
    public void testOneToOneGenericRecordJoin() {

        TestStream<GenericRecord> accountsStream = TestStream.create(AvroCoder.of(TestDataProvider.AccountSchema))
                .addElements(TestDataProvider.getGenericAccount(1L, 1L))
                .advanceWatermarkToInfinity();

        TestStream<GenericRecord> transactionsStream = TestStream.create(AvroCoder.of(TestDataProvider.TransactionDetailSchema))
                .addElements(TestDataProvider.getGenericTransactionDetail(1L, 1L))
                .advanceWatermarkToInfinity();

        List<KV<Long, KV<GenericRecord, GenericRecord>>> expectedResult = new ArrayList<>();
        expectedResult.add(getJoinedRecord(1L, 1L, 1L));

        testPipeline(accountsStream, transactionsStream, expectedResult,
                null, null, JoinType.Inner);

    }

    @Test
    public void testOneToManyGenericRecordJoin() {

        TestStream<GenericRecord> accountsStream = TestStream.create(AvroCoder.of(TestDataProvider.AccountSchema))
                .addElements(TestDataProvider.getGenericAccount(1L, 1L))
                .addElements(TestDataProvider.getGenericAccount(2L, 1L))
                .advanceWatermarkToInfinity();

        TestStream<GenericRecord> transactionsStream = TestStream.create(AvroCoder.of(TestDataProvider.TransactionDetailSchema))
                .addElements(TestDataProvider.getGenericTransactionDetail(1L, 1L))
                .addElements(TestDataProvider.getGenericTransactionDetail(1L, 2L))
                .addElements(TestDataProvider.getGenericTransactionDetail(2L, 3L))
                .advanceWatermarkToInfinity();

        List<KV<Long, KV<GenericRecord, GenericRecord>>> expectedResult = new ArrayList<>();
        expectedResult.add(getJoinedRecord(1L, 1L, 1L));
        expectedResult.add(getJoinedRecord(1L, 1L, 2L));
        expectedResult.add(getJoinedRecord(2L, 1L, 3L));

        testPipeline(accountsStream, transactionsStream, expectedResult,
                null, null, JoinType.Inner);
    }

    @Test
    public void testManyToManyGenericRecordJoin() {

        TestStream<GenericRecord> accountsStream = TestStream.create(AvroCoder.of(TestDataProvider.AccountSchema))
                .addElements(TestDataProvider.getGenericAccount(1L, 1L))
                .addElements(TestDataProvider.getGenericAccount(1L, 1L))
                .addElements(TestDataProvider.getGenericAccount(2L, 1L))
                .addElements(TestDataProvider.getGenericAccount(2L, 1L))
                .advanceWatermarkToInfinity();

        TestStream<GenericRecord> transactionsStream = TestStream.create(AvroCoder.of(TestDataProvider.TransactionDetailSchema))
                .addElements(TestDataProvider.getGenericTransactionDetail(1L, 1L))
                .addElements(TestDataProvider.getGenericTransactionDetail(1L, 2L))
                .addElements(TestDataProvider.getGenericTransactionDetail(2L, 3L))
                .advanceWatermarkToInfinity();

        List<KV<Long, KV<GenericRecord, GenericRecord>>> expectedResult = new ArrayList<>();
        expectedResult.add(getJoinedRecord(1L, 1L, 1L));
        expectedResult.add(getJoinedRecord(1L, 1L, 2L));
        expectedResult.add(getJoinedRecord(2L, 1L, 3L));

        testPipeline(accountsStream, transactionsStream, expectedResult,
                null, null, JoinType.Inner);
    }

    @Test
    public void testNotMatchingLeftGenericRecordJoin() {

        TestStream<GenericRecord> accountsStream = TestStream.create(AvroCoder.of(TestDataProvider.AccountSchema))
                .addElements(TestDataProvider.getGenericAccount(1L, 1L))
                .addElements(TestDataProvider.getGenericAccount(1L, 1L))
                .addElements(TestDataProvider.getGenericAccount(2L, 1L))
                .addElements(TestDataProvider.getGenericAccount(3L, 1L))
                .advanceWatermarkToInfinity();

        TestStream<GenericRecord> transactionsStream = TestStream.create(AvroCoder.of(TestDataProvider.TransactionDetailSchema))
                .addElements(TestDataProvider.getGenericTransactionDetail(1L, 1L))
                .addElements(TestDataProvider.getGenericTransactionDetail(1L, 2L))
                .addElements(TestDataProvider.getGenericTransactionDetail(2L, 3L))
                .advanceWatermarkToInfinity();

        List<KV<Long, KV<GenericRecord, GenericRecord>>> expectedResult = new ArrayList<>();
        expectedResult.add(getJoinedRecord(1L, 1L, 1L));
        expectedResult.add(getJoinedRecord(1L, 1L, 2L));
        expectedResult.add(getJoinedRecord(2L, 1L, 3L));
        List<GenericRecord> leftDroppedElements = new ArrayList<>();
        leftDroppedElements.add(TestDataProvider.getGenericAccount(3L, 1L));

        testPipeline(accountsStream, transactionsStream, expectedResult,
                leftDroppedElements, null, JoinType.Inner);
    }

    @Test
    public void testNotMatchingRightGenericRecordJoin() {

        TestStream<GenericRecord> accountsStream = TestStream.create(AvroCoder.of(TestDataProvider.AccountSchema))
                .addElements(TestDataProvider.getGenericAccount(1L, 1L))
                .addElements(TestDataProvider.getGenericAccount(1L, 1L))
                .advanceWatermarkToInfinity();

        TestStream<GenericRecord> transactionsStream = TestStream.create(AvroCoder.of(TestDataProvider.TransactionDetailSchema))
                .addElements(TestDataProvider.getGenericTransactionDetail(1L, 1L))
                .addElements(TestDataProvider.getGenericTransactionDetail(1L, 2L))
                .addElements(TestDataProvider.getGenericTransactionDetail(3L, 3L))
                .advanceWatermarkToInfinity();

        List<KV<Long, KV<GenericRecord, GenericRecord>>> expectedResult = new ArrayList<>();
        expectedResult.add(getJoinedRecord(1L, 1L, 1L));
        expectedResult.add(getJoinedRecord(1L, 1L, 2L));
        List<GenericRecord> rightDroppedElements = new ArrayList<>();
        rightDroppedElements.add(TestDataProvider.getGenericTransactionDetail(3L, 3L));

        testPipeline(accountsStream, transactionsStream, expectedResult,
                null, rightDroppedElements, JoinType.Inner);
    }

    @Test
    public void testLateRightGenericRecordJoin() {

        Instant now = Instant.now();

        TestStream<GenericRecord> accountsStream = TestStream.create(AvroCoder.of(TestDataProvider.AccountSchema))
                .addElements(TimestampedValue.of(TestDataProvider.getGenericAccount(3L, 1L), now))
                .addElements(TimestampedValue.of(TestDataProvider.getGenericAccount(1L, 1L), now))
                .advanceWatermarkTo(now.plus(Duration.standardSeconds(25)))
                .addElements(TimestampedValue.of(TestDataProvider.getGenericAccount(2L, 1L), now.plus(Duration.standardSeconds(20))))
                .addElements(TimestampedValue.of(TestDataProvider.getGenericAccount(1L, 1L), now.plus(Duration.standardSeconds(25))))
                .advanceWatermarkToInfinity();

        TestStream<GenericRecord> transactionsStream = TestStream.create(AvroCoder.of(TestDataProvider.TransactionDetailSchema))
                .addElements(TimestampedValue.of(TestDataProvider.getGenericTransactionDetail(1L, 1L), now))
                .advanceWatermarkTo(now.plus(Duration.standardSeconds(25)))
                .addElements(TimestampedValue.of(TestDataProvider.getGenericTransactionDetail(2L, 2L), now.plus(Duration.standardSeconds(30))))
                .advanceWatermarkTo(now.plus(Duration.standardSeconds(40)))
                .addElements(TimestampedValue.of(TestDataProvider.getGenericTransactionDetail(3L, 3L), now.plus(Duration.standardSeconds(35))))
                .advanceWatermarkToInfinity();

        List<KV<Long, KV<GenericRecord, GenericRecord>>> expectedResult = new ArrayList<>();
        expectedResult.add(getJoinedRecord(1L, 1L, 1L));
        expectedResult.add(getJoinedRecord(2L, 1L, 2L));
        List<GenericRecord> leftDroppedElements = new ArrayList<>();
        leftDroppedElements.add(TestDataProvider.getGenericAccount(1L, 1L));
        leftDroppedElements.add(TestDataProvider.getGenericAccount(3L, 1L));
        List<GenericRecord> rightDroppedElements = new ArrayList<>();
        rightDroppedElements.add(TestDataProvider.getGenericTransactionDetail(3L, 3L));

        testPipeline(accountsStream, transactionsStream, expectedResult,
                leftDroppedElements, rightDroppedElements, JoinType.Inner);
    }

    @Test
    public void testLeftOneToOneGenericRecordJoin() {

        TestStream<GenericRecord> accountsStream = TestStream.create(AvroCoder.of(TestDataProvider.AccountSchema))
                .addElements(TestDataProvider.getGenericAccount(1L, 1L))
                .advanceWatermarkToInfinity();

        TestStream<GenericRecord> transactionsStream = TestStream.create(AvroCoder.of(TestDataProvider.TransactionDetailSchema))
                .addElements(TestDataProvider.getGenericTransactionDetail(1L, 1L))
                .advanceWatermarkToInfinity();

        List<KV<Long, KV<GenericRecord, GenericRecord>>> expectedResult = new ArrayList<>();
        expectedResult.add(getJoinedRecord(1L, 1L, 1L));

        testPipeline(accountsStream, transactionsStream, expectedResult,
                null, null, JoinType.Left);

    }

    @Test
    public void testLeftOneToManyGenericRecordJoin() {

        TestStream<GenericRecord> accountsStream = TestStream.create(AvroCoder.of(TestDataProvider.AccountSchema))
                .addElements(TestDataProvider.getGenericAccount(1L, 1L))
                .addElements(TestDataProvider.getGenericAccount(2L, 1L))
                .advanceWatermarkToInfinity();

        TestStream<GenericRecord> transactionsStream = TestStream.create(AvroCoder.of(TestDataProvider.TransactionDetailSchema))
                .addElements(TestDataProvider.getGenericTransactionDetail(1L, 1L))
                .addElements(TestDataProvider.getGenericTransactionDetail(1L, 2L))
                .addElements(TestDataProvider.getGenericTransactionDetail(2L, 3L))
                .advanceWatermarkToInfinity();

        List<KV<Long, KV<GenericRecord, GenericRecord>>> expectedResult = new ArrayList<>();
        expectedResult.add(getJoinedRecord(1L, 1L, 1L));
        expectedResult.add(getJoinedRecord(1L, 1L, 2L));
        expectedResult.add(getJoinedRecord(2L, 1L, 3L));

        testPipeline(accountsStream, transactionsStream, expectedResult,
                null, null, JoinType.Left);
    }

    @Test
    public void testLeftManyToManyGenericRecordJoin() {

        TestStream<GenericRecord> accountsStream = TestStream.create(AvroCoder.of(TestDataProvider.AccountSchema))
                .addElements(TestDataProvider.getGenericAccount(1L, 1L))
                .addElements(TestDataProvider.getGenericAccount(1L, 1L))
                .addElements(TestDataProvider.getGenericAccount(2L, 1L))
                .addElements(TestDataProvider.getGenericAccount(2L, 1L))
                .advanceWatermarkToInfinity();

        TestStream<GenericRecord> transactionsStream = TestStream.create(AvroCoder.of(TestDataProvider.TransactionDetailSchema))
                .addElements(TestDataProvider.getGenericTransactionDetail(1L, 1L))
                .addElements(TestDataProvider.getGenericTransactionDetail(1L, 2L))
                .addElements(TestDataProvider.getGenericTransactionDetail(2L, 3L))
                .advanceWatermarkToInfinity();

        List<KV<Long, KV<GenericRecord, GenericRecord>>> expectedResult = new ArrayList<>();
        expectedResult.add(getJoinedRecord(1L, 1L, 1L));
        expectedResult.add(getJoinedRecord(1L, 1L, 2L));
        expectedResult.add(getJoinedRecord(2L, 1L, 3L));

        testPipeline(accountsStream, transactionsStream, expectedResult,
                null, null, JoinType.Inner);
    }

    @Test
    public void testLeftNotMatchingLeftGenericRecordJoin() {

        TestStream<GenericRecord> accountsStream = TestStream.create(AvroCoder.of(TestDataProvider.AccountSchema))
                .addElements(TestDataProvider.getGenericAccount(1L, 1L))
                .addElements(TestDataProvider.getGenericAccount(1L, 1L))
                .addElements(TestDataProvider.getGenericAccount(2L, 1L))
                .addElements(TestDataProvider.getGenericAccount(3L, 1L))
                .advanceWatermarkToInfinity();

        TestStream<GenericRecord> transactionsStream = TestStream.create(AvroCoder.of(TestDataProvider.TransactionDetailSchema))
                .addElements(TestDataProvider.getGenericTransactionDetail(1L, 1L))
                .addElements(TestDataProvider.getGenericTransactionDetail(1L, 2L))
                .addElements(TestDataProvider.getGenericTransactionDetail(2L, 3L))
                .advanceWatermarkToInfinity();

        List<KV<Long, KV<GenericRecord, GenericRecord>>> expectedResult = new ArrayList<>();
        expectedResult.add(getJoinedRecord(1L, 1L, 1L));
        expectedResult.add(getJoinedRecord(1L, 1L, 2L));
        expectedResult.add(getJoinedRecord(2L, 1L, 3L));
        expectedResult.add(getJoinedRecord(3L, 1L, -1L));

        testPipeline(accountsStream, transactionsStream, expectedResult,
                null, null, JoinType.Left);
    }

    @Test
    public void testLeftNotMatchingRightGenericRecordJoin() {

        TestStream<GenericRecord> accountsStream = TestStream.create(AvroCoder.of(TestDataProvider.AccountSchema))
                .addElements(TestDataProvider.getGenericAccount(1L, 1L))
                .addElements(TestDataProvider.getGenericAccount(1L, 1L))
                .advanceWatermarkToInfinity();

        TestStream<GenericRecord> transactionsStream = TestStream.create(AvroCoder.of(TestDataProvider.TransactionDetailSchema))
                .addElements(TestDataProvider.getGenericTransactionDetail(1L, 1L))
                .addElements(TestDataProvider.getGenericTransactionDetail(1L, 2L))
                .addElements(TestDataProvider.getGenericTransactionDetail(3L, 3L))
                .advanceWatermarkToInfinity();

        List<KV<Long, KV<GenericRecord, GenericRecord>>> expectedResult = new ArrayList<>();
        expectedResult.add(getJoinedRecord(1L, 1L, 1L));
        expectedResult.add(getJoinedRecord(1L, 1L, 2L));
        List<GenericRecord> rightDroppedElements = new ArrayList<>();
        rightDroppedElements.add(TestDataProvider.getGenericTransactionDetail(3L, 3L));

        testPipeline(accountsStream, transactionsStream, expectedResult,
                null, rightDroppedElements, JoinType.Left);
    }

    @Test
    public void testLeftLateRightGenericRecordJoin() {

        Instant now = Instant.now();

        TestStream<GenericRecord> accountsStream = TestStream.create(AvroCoder.of(TestDataProvider.AccountSchema))
                .advanceWatermarkTo(now.plus(Duration.standardSeconds(10)))
                .addElements(TimestampedValue.of(TestDataProvider.getGenericAccount(3L, 1L), now))
                .advanceWatermarkTo(now.plus(Duration.standardSeconds(30)))
                .addElements(TimestampedValue.of(TestDataProvider.getGenericAccount(1L, 1L), now))
                .advanceWatermarkTo(now.plus(Duration.standardSeconds(40)))
                .advanceProcessingTime(Duration.standardSeconds(680))
                .addElements(TimestampedValue.of(TestDataProvider.getGenericAccount(2L, 1L), now.plus(Duration.standardSeconds(20))))
                .advanceWatermarkTo(now.plus(Duration.standardSeconds(50)))
                .addElements(TimestampedValue.of(TestDataProvider.getGenericAccount(1L, 1L), now.plus(Duration.standardSeconds(25))))
                .advanceWatermarkTo(now.plus(Duration.standardSeconds(65)))
                .addElements(TimestampedValue.of(TestDataProvider.getGenericAccount(6L, 1L), now.plus(Duration.standardSeconds(120))))
                .advanceWatermarkTo(now.plus(Duration.standardSeconds(120)))
                .addElements(TimestampedValue.of(TestDataProvider.getGenericAccount(6L, 1L), now.plus(Duration.standardSeconds(130))))
                .advanceWatermarkTo(now.plus(Duration.standardSeconds(130)))
                .advanceWatermarkToInfinity();

        TestStream<GenericRecord> transactionsStream = TestStream.create(AvroCoder.of(TestDataProvider.TransactionDetailSchema))
                .advanceWatermarkTo(now.plus(Duration.standardSeconds(1)))
                .advanceProcessingTime(Duration.standardSeconds(5))
                .advanceWatermarkTo(now.plus(Duration.standardSeconds(5)))
                .addElements(TimestampedValue.of(TestDataProvider.getGenericTransactionDetail(2L, 2L), now.plus(Duration.standardSeconds(10))))
                .advanceWatermarkTo(now.plus(Duration.standardSeconds(10)))
                .advanceProcessingTime(Duration.standardSeconds(10))
                .addElements(TimestampedValue.of(TestDataProvider.getGenericTransactionDetail(1L, 1L), now.plus(Duration.standardSeconds(20))))
                .advanceWatermarkTo(now.plus(Duration.standardSeconds(20)))
                .advanceProcessingTime(Duration.standardSeconds(600))
                .addElements(TimestampedValue.of(TestDataProvider.getGenericTransactionDetail(3L, 3L), now.plus(Duration.standardSeconds(35))))
                .advanceWatermarkTo(now.plus(Duration.standardSeconds(180)))
                .addElements(TimestampedValue.of(TestDataProvider.getGenericTransactionDetail(3L, 3L), now.plus(Duration.standardSeconds(180))))
                .advanceWatermarkToInfinity();

        List<KV<Long, KV<GenericRecord, GenericRecord>>> expectedResult = new ArrayList<>();
        expectedResult.add(getJoinedRecord(1L, 1L, 1L));
        expectedResult.add(getJoinedRecord(2L, 1L, 2L));
        expectedResult.add(getJoinedRecord(1L, 1L, -1L));
        expectedResult.add(getJoinedRecord(3L, 1L, -1L));
        List<GenericRecord> rightDroppedElements = new ArrayList<>();
        rightDroppedElements.add(TestDataProvider.getGenericTransactionDetail(3L, 3L));

        testPipeline(accountsStream, transactionsStream, expectedResult,
                null, rightDroppedElements, JoinType.Left);
    }

    public void testPipeline(TestStream<GenericRecord> accountsStream,
                             TestStream<GenericRecord> transactionsStream,
                             List<KV<Long, KV<GenericRecord, GenericRecord>>> expectedResult,
                             List<GenericRecord> leftDroppedElements,
                             List<GenericRecord> rightDroppedElements,
                             JoinType joinType) {
        PCollection<KV<Long, GenericRecord>> accounts = p.apply("Create Accounts", accountsStream)
                .apply("Key Accounts", WithKeys.of((SerializableFunction<GenericRecord, Long>) input -> (long) input.get("acid")))
                .setCoder(KvCoder.of(VarLongCoder.of(), AvroCoder.of(TestDataProvider.AccountSchema)));

        PCollection<KV<Long, GenericRecord>> transactions = p.apply("Create Transactions", transactionsStream)
                .apply("Key Transactions", WithKeys.of((SerializableFunction<GenericRecord, Long>) input -> (long) input.get("acid")))
                .setCoder(KvCoder.of(VarLongCoder.of(), AvroCoder.of(TestDataProvider.TransactionDetailSchema)));

        List<PCollection<GenericRecord>> droppedLeftCollection = new ArrayList<>();
        List<PCollection<GenericRecord>> droppedRightCollection = new ArrayList<>();
        PCollection<KV<Long, KV<GenericRecord, GenericRecord>>> joinedRecords = null;
        if (joinType == JoinType.Inner)
            joinedRecords = accounts.apply("Join with Transactions",
                            OneToManyJoin.inner(transactions))
                    .droppedElementsTo(droppedLeftCollection, droppedRightCollection
                    );
        else if (joinType == JoinType.Left)
            joinedRecords = accounts.apply("Join with Transactions",
                            OneToManyJoin.left(transactions))
                    .droppedElementsTo(droppedLeftCollection, droppedRightCollection
                    );
        Assert.assertNotNull(joinedRecords);
        PAssert.that(joinedRecords).containsInAnyOrder(expectedResult);

        if (leftDroppedElements != null)
            PAssert.that(droppedLeftCollection.get(0)).containsInAnyOrder(leftDroppedElements);
        else
            PAssert.that(droppedLeftCollection.get(0)).empty();
        if (rightDroppedElements != null)
            PAssert.that(droppedRightCollection.get(0)).containsInAnyOrder(rightDroppedElements);
        else
            PAssert.that(droppedRightCollection.get(0)).empty();

        p.run().waitUntilFinish();
    }

    @Test
    public void testMultipleJoin() {
        Instant now = Instant.now();

        TestStream<GenericRecord> accountsStream = TestStream.create(AvroCoder.of(TestDataProvider.AccountSchema))
                .addElements(TimestampedValue.of(TestDataProvider.getGenericAccount(3L, 1L), now))
                .addElements(TimestampedValue.of(TestDataProvider.getGenericAccount(1L, 1L), now))
                .advanceWatermarkTo(now.plus(Duration.standardSeconds(25)))
                .addElements(TimestampedValue.of(TestDataProvider.getGenericAccount(2L, 1L), now.plus(Duration.standardSeconds(20))))
                .addElements(TimestampedValue.of(TestDataProvider.getGenericAccount(1L, 1L), now.plus(Duration.standardSeconds(25))))
                .advanceWatermarkToInfinity();

        TestStream<GenericRecord> transactionsStream = TestStream.create(AvroCoder.of(TestDataProvider.TransactionDetailSchema))
                .addElements(TimestampedValue.of(TestDataProvider.getGenericTransactionDetail(1L, 1L), now))
                .advanceWatermarkTo(now.plus(Duration.standardSeconds(25)))
                .addElements(TimestampedValue.of(TestDataProvider.getGenericTransactionDetail(2L, 2L), now.plus(Duration.standardSeconds(30))))
                .advanceWatermarkTo(now.plus(Duration.standardSeconds(40)))
                .addElements(TimestampedValue.of(TestDataProvider.getGenericTransactionDetail(3L, 3L), now.plus(Duration.standardSeconds(35))))
                .advanceWatermarkToInfinity();

        TestStream<GenericRecord> transactionsHeaderStream = TestStream.create(AvroCoder.of(TestDataProvider.TransactionHeaderSchema))
                .addElements(TimestampedValue.of(TestDataProvider.getGenericHeader(1L), now))
                .advanceWatermarkTo(now.plus(Duration.standardSeconds(10)))
                .addElements(TimestampedValue.of(TestDataProvider.getGenericHeader(2L), now.plus(Duration.standardSeconds(30))))
                .advanceWatermarkTo(now.plus(Duration.standardSeconds(80)))
                .addElements(TimestampedValue.of(TestDataProvider.getGenericHeader(4L), now.plus(Duration.standardSeconds(80))))
                .addElements(TimestampedValue.of(TestDataProvider.getGenericHeader(3L), now.plus(Duration.standardSeconds(90))))
                .advanceWatermarkToInfinity();

        PCollection<KV<Long, GenericRecord>> accounts = p.apply("Create Accounts", accountsStream)
                .apply("Key Accounts", WithKeys.of((SerializableFunction<GenericRecord, Long>) input -> (long) input.get("acid")))
                .setCoder(KvCoder.of(NullableCoder.of(VarLongCoder.of()), AvroCoder.of(TestDataProvider.AccountSchema)));

        PCollection<KV<Long, GenericRecord>> transactions = p.apply("Create Transactions", transactionsStream)
                .apply("Key Transactions", WithKeys.of((SerializableFunction<GenericRecord, Long>) input -> (long) input.get("acid")))
                .setCoder(KvCoder.of(NullableCoder.of(VarLongCoder.of()), AvroCoder.of(TestDataProvider.TransactionDetailSchema)));

        PCollection<KV<Long, GenericRecord>> transactionHeaders = p.apply("Create Transaction Headers", transactionsHeaderStream)
                .apply("Key Transaction Headers", WithKeys.of((SerializableFunction<GenericRecord, Long>) input -> (long) input.get("tran_id")))
                .setCoder(KvCoder.of(NullableCoder.of(VarLongCoder.of()), AvroCoder.of(TestDataProvider.TransactionHeaderSchema)));

        List<PCollection<GenericRecord>> droppedLeftCollection = new ArrayList<>();
        List<PCollection<GenericRecord>> droppedRightCollection = new ArrayList<>();
        PCollection<KV<Long, KV<GenericRecord, GenericRecord>>> joinedRecords =
                accounts.apply("Join with Transactions",
                                OneToManyJoin.inner(transactions))
                        .droppedElementsTo(droppedLeftCollection, droppedRightCollection
                        ).apply("ReKey", ParDo.of(getReKeyFn()));

        PCollection<KV<Long, KV<GenericRecord, KV<GenericRecord, GenericRecord>>>> resultJoin = transactionHeaders
                .apply("Join with Header", OneToManyJoin.inner(joinedRecords)).droppedElementsIgnored();

        List<KV<Long, KV<GenericRecord, KV<GenericRecord, GenericRecord>>>> expectedResult = new ArrayList<>();
        expectedResult.add(getMultiJoinedRecord(1L, 1L, 1L));
        expectedResult.add(getMultiJoinedRecord(2L, 1L, 2L));
        PAssert.that(resultJoin).containsInAnyOrder(expectedResult);

        p.run().waitUntilFinish();
    }

    @Test
    public void testLeftMultipleJoin() {
        Instant now = Instant.now();

        TestStream<GenericRecord> accountsStream = TestStream.create(AvroCoder.of(TestDataProvider.AccountSchema))
                .addElements(TimestampedValue.of(TestDataProvider.getGenericAccount(3L, 1L), now))
                .addElements(TimestampedValue.of(TestDataProvider.getGenericAccount(1L, 1L), now))
                .addElements(TimestampedValue.of(TestDataProvider.getGenericAccount(2L, 1L), now.plus(Duration.standardSeconds(20))))
                .advanceWatermarkTo(now.plus(Duration.standardSeconds(25)))
                .addElements(TimestampedValue.of(TestDataProvider.getGenericAccount(1L, 1L), now.plus(Duration.standardSeconds(25))))
                .advanceWatermarkToInfinity();

        TestStream<GenericRecord> transactionsStream = TestStream.create(AvroCoder.of(TestDataProvider.TransactionDetailSchema))
                .addElements(TimestampedValue.of(TestDataProvider.getGenericTransactionDetail(1L, 1L), now))
                .addElements(TimestampedValue.of(TestDataProvider.getGenericTransactionDetail(2L, 2L), now.plus(Duration.standardSeconds(30))))
                .advanceWatermarkTo(now.plus(Duration.standardSeconds(40)))
                .addElements(TimestampedValue.of(TestDataProvider.getGenericTransactionDetail(3L, 3L), now.plus(Duration.standardSeconds(35))))
                .advanceWatermarkToInfinity();

        TestStream<GenericRecord> transactionsHeaderStream = TestStream.create(AvroCoder.of(TestDataProvider.TransactionHeaderSchema))
                .addElements(TimestampedValue.of(TestDataProvider.getGenericHeader(1L), now))
                .addElements(TimestampedValue.of(TestDataProvider.getGenericHeader(2L), now.plus(Duration.standardSeconds(30))))
                .advanceWatermarkTo(now.plus(Duration.standardSeconds(60)))
                .addElements(TimestampedValue.of(TestDataProvider.getGenericHeader(3L), now.plus(Duration.standardSeconds(50))))
                .advanceWatermarkToInfinity();

        PCollection<KV<Long, GenericRecord>> accounts = p.apply("Create Accounts", accountsStream)
                .apply("Key Accounts", WithKeys.of((SerializableFunction<GenericRecord, Long>) input -> (long) input.get("acid")))
                .setCoder(KvCoder.of(NullableCoder.of(VarLongCoder.of()), NullableCoder.of(AvroCoder.of(TestDataProvider.AccountSchema))));

        PCollection<KV<Long, GenericRecord>> transactions = p.apply("Create Transactions", transactionsStream)
                .apply("Key Transactions", WithKeys.of((SerializableFunction<GenericRecord, Long>) input -> (long) input.get("acid")))
                .setCoder(KvCoder.of(NullableCoder.of(VarLongCoder.of()), NullableCoder.of(AvroCoder.of(TestDataProvider.TransactionDetailSchema))));

        PCollection<KV<Long, GenericRecord>> transactionHeaders = p.apply("Create Transaction Headers", transactionsHeaderStream)
                .apply("Key Transaction Headers", WithKeys.of((SerializableFunction<GenericRecord, Long>) input -> (long) input.get("tran_id")))
                .setCoder(KvCoder.of(NullableCoder.of(VarLongCoder.of()), NullableCoder.of(AvroCoder.of(TestDataProvider.TransactionHeaderSchema))));

        List<PCollection<GenericRecord>> droppedLeftCollection = new ArrayList<>();
        List<PCollection<GenericRecord>> droppedRightCollection = new ArrayList<>();
        PCollection<KV<Long, KV<GenericRecord, GenericRecord>>> joinedRecords =
                accounts.apply("Join with Transactions",
                                OneToManyJoin.left(transactions))
                        .droppedElementsTo(droppedLeftCollection, droppedRightCollection
                        ).apply("ReKey", ParDo.of(getReKeyFn()));
        //joinedRecords.apply("Print 1",ParDo.of(getGenericJoinLogger()));
        PCollection<KV<Long, KV<GenericRecord, KV<GenericRecord, GenericRecord>>>> resultJoin = transactionHeaders
                .apply("Join with Header", OneToManyJoin.left(joinedRecords)).droppedElementsIgnored();
        //resultJoin.apply("Print",ParDo.of(getMultiGenericJoinLogger()));

        List<KV<Long, KV<GenericRecord, KV<GenericRecord, GenericRecord>>>> expectedResult = new ArrayList<>();
        expectedResult.add(getMultiJoinedRecord(1L, 1L, 1L));
        expectedResult.add(getMultiJoinedRecord(2L, 1L, 2L));
        expectedResult.add(getMultiJoinedRecord(-1L, -1L, 3L));
        PAssert.that(resultJoin).containsInAnyOrder(expectedResult);

        p.run().waitUntilFinish();
    }

    @Test
    public void testExpiryJoin() {
        Instant now = Instant.now();

        TestStream<GenericRecord> accountsStream = TestStream.create(AvroCoder.of(TestDataProvider.AccountSchema))
                .addElements(TestDataProvider.getGenericAccount(1L, 1L))
                .advanceWatermarkToInfinity();

        TestStream<GenericRecord> transactionsStream = TestStream.create(AvroCoder.of(TestDataProvider.TransactionDetailSchema))
                .addElements(TestDataProvider.getGenericTransactionDetail(1L, 1L))
                .advanceWatermarkToInfinity();

        PCollection<KV<Long, GenericRecord>> accounts = p.apply("Create Accounts", accountsStream)
                .apply("Key Accounts", WithKeys.of((SerializableFunction<GenericRecord, Long>) input -> (long) input.get("acid")))
                .setCoder(KvCoder.of(NullableCoder.of(VarLongCoder.of()), AvroCoder.of(TestDataProvider.AccountSchema)));

        PCollection<KV<Long, GenericRecord>> transactions = p.apply("Create Transactions", transactionsStream)
                .apply("Key Transactions", WithKeys.of((SerializableFunction<GenericRecord, Long>) input -> (long) input.get("acid")))
                .setCoder(KvCoder.of(NullableCoder.of(VarLongCoder.of()), AvroCoder.of(TestDataProvider.TransactionDetailSchema)));


        List<PCollection<GenericRecord>> droppedLeftCollection = new ArrayList<>();
        List<PCollection<GenericRecord>> droppedRightCollection = new ArrayList<>();
        PCollection<KV<Long, KV<GenericRecord, GenericRecord>>> joinedRecords =
                accounts.apply("Join with Transactions",
                                OneToManyJoin.inner(transactions)
                                        .withLeftStateExpireDuration(Duration.standardSeconds(60)))
                        .droppedElementsTo(droppedLeftCollection, droppedRightCollection
                        ).apply("ReKey", ParDo.of(getReKeyFn()));

        List<KV<Long, KV<GenericRecord, GenericRecord>>> expectedResult = new ArrayList<>();
        expectedResult.add(getJoinedRecord(1L, 1L, 1L));
        PAssert.that(joinedRecords).containsInAnyOrder(expectedResult);

        p.run().waitUntilFinish();
    }

    public KV<Long, KV<GenericRecord, GenericRecord>> getJoinedRecord(long acid, long cust_id, long tran_id) {
        return KV.of(acid, KV.of(TestDataProvider.getGenericAccount(acid, cust_id),
                TestDataProvider.getGenericTransactionDetail(acid, tran_id)));
    }

    public KV<Long, KV<GenericRecord, KV<GenericRecord, GenericRecord>>> getMultiJoinedRecord(long acid, long cust_id, long tran_id) {
        if (acid < 0)
            return KV.of(tran_id, KV.of(TestDataProvider.getGenericHeader(tran_id), null));
        return KV.of(tran_id, KV.of(TestDataProvider.getGenericHeader(tran_id), KV.of(TestDataProvider.getGenericAccount(acid, cust_id),
                TestDataProvider.getGenericTransactionDetail(acid, tran_id))));
    }


}
