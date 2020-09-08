package leap.data.beam.transforms.aggregate;

import leap.data.beam.TestDataProvider;
import leap.data.beam.transforms.convert.GenericRecordToRow;
import leap.data.beam.transforms.convert.RowToGenericRecord;
import leap.data.beam.transforms.join.OneToManyJoin;
import leap.data.beam.transforms.join.UnNestJoinedGenericRecords;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.schemas.transforms.Group;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Duration;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LeapGroupTest {
    private static final Logger logger = LoggerFactory.getLogger(LeapGroupTest.class);

    @Rule
    public TestPipeline p = TestPipeline.create();

    @Test
    public void test(){
        TestStream<GenericRecord> accountsStream = TestStream.create(AvroCoder.of(TestDataProvider.AccountSchema))
                .addElements(TestDataProvider.getGenericAccount(1L,1L))
                .addElements(TestDataProvider.getGenericAccount(2L,1L))
                .advanceWatermarkToInfinity();

        TestStream<GenericRecord> transactionsStream = TestStream.create(AvroCoder.of(TestDataProvider.TransactionDetailSchema))
                .addElements(TestDataProvider.getGenericTransactionDetail(1L,1L))
                .addElements(TestDataProvider.getGenericTransactionDetail(1L,2L))
                .addElements(TestDataProvider.getGenericTransactionDetail(2L,3L))
                .advanceWatermarkToInfinity();

        PCollection<KV<Long,GenericRecord>> accounts = p.apply("Create Accounts", accountsStream)
                .apply("Key Accounts", WithKeys.of((SerializableFunction<GenericRecord, Long>) input -> (long)input.get("acid")))
                .setCoder(KvCoder.of(VarLongCoder.of(),AvroCoder.of(TestDataProvider.AccountSchema)));

        PCollection<KV<Long,GenericRecord>> transactions = p.apply("Create Transactions", transactionsStream)
                .apply("Key Transactions",WithKeys.of((SerializableFunction<GenericRecord, Long>) input -> (long)input.get("acid")))
                .setCoder(KvCoder.of(VarLongCoder.of(),AvroCoder.of(TestDataProvider.TransactionDetailSchema)));

        PCollection<GenericRecord> joinedRecords =
                accounts.apply("Join with Transactions",
                        OneToManyJoin.inner(transactions))
                        .droppedElementsIgnored()
                .apply(Values.create()).apply(Keys.create());

        accounts.apply("Join with Transactions 2",
                        OneToManyJoin.inner(transactions))
                        .droppedElementsIgnored()
                .apply("UnNest", UnNestJoinedGenericRecords.of())
                        .apply("Log 2", ParDo.of(getLogger()));

        joinedRecords
                .apply("Window", Window.<GenericRecord>into(Sessions.withGapDuration(Duration.standardSeconds(1)))
                    .triggering(AfterWatermark.pastEndOfWindow())
                    .withAllowedLateness(Duration.ZERO))
                .apply("ToRows", GenericRecordToRow.convert(TestDataProvider.AccountSchema))
                .apply("Aggregate", Group.<Row>byFieldNames("acid")
                    .aggregateField("cust_id", Max.ofLongs(),"max_cust_id"))
                .apply("ToGeneric", RowToGenericRecord.convert());
                //.apply("Log", ParDo.of(getLogger()));


        p.run().waitUntilFinish();
    }

    private static DoFn<GenericRecord, Row> toRow(){
        return new DoFn<GenericRecord, Row>() {
            @ProcessElement
            public void processElement(ProcessContext context) {
                context.output(AvroUtils.toBeamRowStrict(context.element(), null));
            }
        };
    }

    private static DoFn<Row, GenericRecord> toGeneric(){
        return new DoFn<Row, GenericRecord>() {
            @ProcessElement
            public void processElement(ProcessContext context) {
                context.output(AvroUtils.toGenericRecord(context.element(), null));
            }
        };
    }

    public static DoFn<Object, String> getLogger(){
        return new DoFn<Object, String>() {
            @ProcessElement
            public void processElement(@Element Object element){
                logger.error("{}",element);
            }
        };
    }
}
