package leap.data.beam.io;

import leap.data.beam.configuration.KafkaPipelineOptions;
import leap.data.beam.entity.AccountCreatedEvent;
import leap.data.beam.entity.AccountCreatedEventReflect;
import leap.data.beam.entity.AccountReflect;
import leap.data.framework.core.serialization.LeapSerializerConfig;
import leap.data.framework.extension.confluent.avro.LeapAvroSerializer;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

@SuppressWarnings("WrapperTypeMayBePrimitive")
public class LeapKafkaIOTest {
    private static final KafkaPipelineOptions options;

    static {
        options = PipelineOptionsFactory.fromArgs("--kafkaBootstrapServers=none",
                "--kafkaSchemaRegistryUrl=mock://",
                "--kafkaSecurityProtocol=PLAINTEXT",
                "--kafkaSaslMechanism=")
                .as(KafkaPipelineOptions.class);
    }

    @Rule
    public TestPipeline p = TestPipeline.fromOptions(options);


    public static void addCountingAsserts(
            PCollection<Long> input, long count, long uniqueCount, long min, long max) {

        PAssert.thatSingleton(input.apply("Count", Count.globally())).isEqualTo(count);

        PAssert.thatSingleton(input.apply(Distinct.create()).apply("UniqueCount", Count.globally()))
                .isEqualTo(uniqueCount);

        PAssert.thatSingleton(input.apply("Min", Min.globally())).isEqualTo(min);

        PAssert.thatSingleton(input.apply("Max", Max.globally())).isEqualTo(max);
    }

    private byte[] serializedGenericAccountCreatedRecord;
    private byte[] serializedGenericAccountBalanceUpdatedRecord;


    @Before
    public void setUp() {
        GenericData genericData = GenericData.get();
        genericData.addLogicalTypeConversion(new TimeConversions.DateConversion());

        Map<String, Object> config = new HashMap<>();
        config.put("schema.registry.url", "mock://");
        LeapAvroSerializer serializer = new LeapAvroSerializer(new LeapSerializerConfig(config));
        serializedGenericAccountCreatedRecord = serializer.serialize("account-created",
                AvroTestDataProvider.genericRecordDataEventAccountCreated(0L));
        serializedGenericAccountBalanceUpdatedRecord = serializer.serialize("balance-updated",
                AvroTestDataProvider.genericRecordDataEventAccountBalanceUpdated(1L));

        PipelineOptionsFactory.register(KafkaPipelineOptions.class);
    }

    @Test
    public void testKafkaIOReadGenericDefault() {
        List<String> topics = ImmutableList.of("generic-topic");
        Integer numElements = 100;
        byte[][] values = new byte[1][];
        values[0] = serializedGenericAccountCreatedRecord;
        PCollection<Long> eventIds = p.apply(LeapKafkaIO.readGenericDefault()
                .withTopic("generic-topic")
                .withSchemaName("account-created")
                .withConsumerFactoryFn(new ConsumerFactoryFn(
                        topics, 10, numElements, OffsetResetStrategy.EARLIEST, values))
                .withMaxNumRecords(numElements.longValue())
        ).apply(ParDo.of(extractEventIdDoFn()));
        //All of the elements has 0 as eventId
        //Only one unique record exist, min and max is 0
        addCountingAsserts(eventIds, numElements, 1L, 0L, 0L);
        p.run();
    }

    @Test
    public void testKafkaIOReadGeneric() throws Exception {
        List<String> topics = ImmutableList.of("generic-topic");
        Integer numElements = 100;
        byte[][] values = new byte[1][];
        values[0] = serializedGenericAccountCreatedRecord;
        PCollection<Long> eventIds = p.apply(LeapKafkaIO.<String>readGeneric(options, "account-created")
                .withTopic("generic-topic")
                .withKeyDeserializer(StringDeserializer.class)
                .withConsumerFactoryFn(new ConsumerFactoryFn(
                        topics, 10, numElements, OffsetResetStrategy.EARLIEST, values))
                .withMaxNumRecords(numElements.longValue())
        ).apply(ParDo.of(extractEventIdDoFn()));
        //All of the elements has 0 as eventId
        //Only one unique record exist, min and max is 0
        addCountingAsserts(eventIds, numElements, 1L, 0L, 0L);
        p.run();
    }

    @Test
    public void testKafkaIOReadGenericWithReaderSchema() {
        List<String> topics = ImmutableList.of("generic-topic");
        Integer numElements = 100;
        byte[][] values = new byte[2][];
        values[0] = serializedGenericAccountCreatedRecord;
        values[1] = serializedGenericAccountBalanceUpdatedRecord;
        PCollection<Long> eventIds = p.apply(LeapKafkaIO.readGenericDefault()
                .withTopic("generic-topic")
                .withReaderSchema(AvroTestDataProvider.AVRO_READER_SCHEMA_EVENT_ACCOUNT)
                .withConsumerFactoryFn(new ConsumerFactoryFn(
                        topics, 10, numElements, OffsetResetStrategy.EARLIEST, values
                ))
                .withMaxNumRecords(numElements.longValue())
        ).apply(ParDo.of(extractEventIdDoFn()));
        //Two unique element with different write schema
        addCountingAsserts(eventIds, numElements, 2L, 0L, 1L);
        p.run();
    }

    @Test
    public void testKafkaIOReadSpecific() {
        List<String> topics = ImmutableList.of("specific-topic");
        Integer numElements = 100;
        byte[][] values = new byte[1][];
        values[0] = serializedGenericAccountCreatedRecord;
        PCollection<Long> eventIds = p.apply(LeapKafkaIO.<AccountCreatedEvent>readSpecific()
                .withTopic("specific-topic")
                .withValueCoderClass(AccountCreatedEvent.class)
                .withConsumerFactoryFn(new ConsumerFactoryFn(
                        topics, 10, numElements, OffsetResetStrategy.EARLIEST, values))
                .withMaxNumRecords(numElements.longValue())
        ).apply(ParDo.of(extractSpecificEventIdDoFn()));
        //All of the elements has 0 as eventId
        //Only one unique record exist, min and max is 0
        addCountingAsserts(eventIds, numElements, 1L, 0L, 0L);
        p.run();
    }

    private static DoFn<KafkaRecord<String, GenericRecord>, Long> extractEventIdDoFn() {
        return new DoFn<KafkaRecord<String, GenericRecord>, Long>() {
            @ProcessElement
            public void processElement(@Element KafkaRecord<String, GenericRecord> element, ProcessContext c) {
                c.output((Long) element.getKV().getValue().get("eventId"));
            }
        };
    }

    private static DoFn<KafkaRecord<Long, AccountCreatedEvent>, Long> extractSpecificEventIdDoFn() {
        return new DoFn<KafkaRecord<Long, AccountCreatedEvent>, Long>() {
            @ProcessElement
            public void processElement(@Element KafkaRecord<Long, AccountCreatedEvent> element, ProcessContext c) {
                //logger.error(element.getKV().getValue().toString());
                c.output(element.getKV().getValue().getEventId());
            }
        };
    }

    private PTransform<PBegin, PCollection<KV<String, GenericRecord>>> mkKafkaReadTransform(
            Integer numElements
    ) throws Exception {
        List<String> topics = ImmutableList.of("generic-topic");
        byte[][] values = new byte[1][];
        values[0] = serializedGenericAccountCreatedRecord;
        return LeapKafkaIO.<String>readGeneric(options, AvroTestDataProvider.AVRO_SCHEMA_EVENT_ACCOUNT_CREATED)
                .withTopic("generic-topic")
                .withConsumerFactoryFn(new ConsumerFactoryFn(
                        topics, 10, numElements, OffsetResetStrategy.EARLIEST, values
                ))
                .withKeyDeserializer(StringDeserializer.class)
                .withMaxNumRecords(numElements.longValue())
                .withoutMetadata();
    }

    @Test
    public void testKafkaIOWriteDefault() throws Exception {
        // Simply read from kafka source and write to kafka sink. Then verify the records
        // are correctly published to mock kafka producer.
        int numElements = 1000;
        try (ProducerFactoryFn.MockProducerWrapper<GenericRecord> producerWrapper = new ProducerFactoryFn.MockProducerWrapper<>()) {
            ProducerFactoryFn.ProducerSendCompletionThread<GenericRecord> completionThread =
                    new ProducerFactoryFn.ProducerSendCompletionThread<>(producerWrapper.mockProducer).start();
            String topic = "test";
            p.apply(mkKafkaReadTransform(numElements))
                    .apply(
                            LeapKafkaIO.<GenericRecord>writeValues()
                                    .withTopic(topic)
                                    .withProducerFactoryFn(new ProducerFactoryFn<>(producerWrapper.producerKey)));
            p.run();
            completionThread.shutdown();
            verifyProducerRecords(producerWrapper.mockProducer, topic, numElements, true, false);
        }
    }

    @Test
    public void testKafkaIOWrite() throws Exception {
        // Simply read from kafka source and write to kafka sink. Then verify the records
        // are correctly published to mock kafka producer.
        int numElements = 1;
        try (ProducerFactoryFn.MockProducerWrapper<GenericRecord> producerWrapper =
                     new ProducerFactoryFn.MockProducerWrapper<>()) {
            ProducerFactoryFn.ProducerSendCompletionThread<GenericRecord> completionThread =
                    new ProducerFactoryFn.ProducerSendCompletionThread<>(producerWrapper.mockProducer).start();
            String topic = "test";
            p.apply(mkKafkaReadTransform(numElements))
                    .apply(ParDo.of(extractValuesDoFn()))
                    .apply(
                            LeapKafkaIO.<String, GenericRecord>write(options)
                                    .withTopic(topic)
                                    //.withKeySerializer(StringSerializer.class)
                                    //.withInputTimestamp()
                                    .withProducerFactoryFn(new ProducerFactoryFn<>(producerWrapper.producerKey))
                                    .values()
                    );
            p.run();
            completionThread.shutdown();
            verifyProducerRecords(producerWrapper.mockProducer, topic, numElements, true, false);
        }
    }

    @Test
    public void testKafkaIOReflect() throws Exception {
        // Simply read from kafka source and write to kafka sink. Then verify the records
        // are correctly published to mock kafka producer.
        int numElements = 1000;
        try (ProducerFactoryFn.MockProducerWrapper<AccountCreatedEventReflect> producerWrapper =
                     new ProducerFactoryFn.MockProducerWrapper<>()) {
            ProducerFactoryFn.ProducerSendCompletionThread<AccountCreatedEventReflect> completionThread =
                    new ProducerFactoryFn.ProducerSendCompletionThread<>(producerWrapper.mockProducer).start();
            String topic = "test";
            p.apply(mkKafkaReadTransform(numElements))
                    .apply(ParDo.of(convertToPojo()))
                    .apply(
                            LeapKafkaIO.<String,AccountCreatedEventReflect>write(options)
                                    .withTopic(topic)
                                    //.withKeySerializer(StringSerializer.class)
                                    //.withInputTimestamp()
                                    .withProducerFactoryFn(new ProducerFactoryFn<>(producerWrapper.producerKey))
                                    .values()
                    );
            p.run();
            completionThread.shutdown();
            //verifyProducerRecords(producerWrapper.mockProducer, topic, numElements, true, false);
        }
    }

    private static DoFn<KV<String, GenericRecord>, GenericRecord> extractValuesDoFn() {
        return new DoFn<KV<String, GenericRecord>, GenericRecord>() {
            @ProcessElement
            public void processElement(@Element KV<String, GenericRecord> element, ProcessContext c) {
                c.output(element.getValue());
            }
        };
    }

    private static DoFn<KV<String, GenericRecord>, AccountCreatedEventReflect> convertToPojo() {
        return new DoFn<KV<String, GenericRecord>, AccountCreatedEventReflect>() {
            @ProcessElement
            public void processElement(@Element KV<String, GenericRecord> element, ProcessContext c) {
                GenericRecord value = element.getValue();
                GenericRecord accountValue = (GenericRecord) value.get("data");
                AccountReflect accountReflect = new AccountReflect();
                accountReflect.setAcid((Long) accountValue.get("acid"));
                accountReflect.setBalance((Double) accountValue.get("balance"));
                AccountCreatedEventReflect accountCreatedEventReflect = new AccountCreatedEventReflect();
                accountCreatedEventReflect.setEventId((Long) value.get("eventId"));
                accountCreatedEventReflect.setTraceId((Long) value.get("traceId"));
                accountCreatedEventReflect.setType(value.get("type").toString());
                accountCreatedEventReflect.setData(accountReflect);
                c.output(accountCreatedEventReflect);
            }
        };
    }

    @SuppressWarnings("SameParameterValue")
    private static <V> void verifyProducerRecords(
            MockProducer<String, V> mockProducer,
            String topic,
            int numElements,
            boolean keyIsAbsent,
            boolean verifyTimestamp) {

        // verify that appropriate messages are written to kafka
        List<ProducerRecord<String, V>> sent = mockProducer.history();

        // sort by values
        //sent.sort(Comparator.comparing(ProducerRecord::key));

        for (int i = 0; i < numElements; i++) {
            ProducerRecord<String, V> record = sent.get(i);
            assertEquals(topic, record.topic());
            if (!keyIsAbsent) {
                //noinspection AssertEqualsBetweenInconvertibleTypes
                assertEquals(i, record.key());
            }
            assertEquals(0L, ((GenericRecord)record.value()).get("eventId"));
            if (verifyTimestamp) {
                assertEquals(i, record.timestamp().intValue());
            }
        }
    }
}