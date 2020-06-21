package leap.data.beam.io;

import leap.data.beam.configuration.KafkaPipelineOptions;
import leap.data.framework.core.serialization.LeapSerializerConfig;
import leap.data.framework.core.serialization.avro.AvroDeserializer;
import leap.data.framework.extension.confluent.avro.LeapAvroDeserializer;
import leap.data.framework.extension.confluent.avro.LeapAvroSerializer;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.util.concurrent.Uninterruptibles;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.AbstractMap.SimpleEntry;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

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


    /*
    Mocking Kafka Brokers Start
     */
    private static final Instant LOG_APPEND_START_TIME = new Instant(600 * 1000);
    private static final String TIMESTAMP_START_MILLIS_CONFIG = "test.timestamp.start.millis";
    private static final String TIMESTAMP_TYPE_CONFIG = "test.timestamp.type";

    // Update mock consumer with records distributed among the given topics, each with given number
    // of partitions. Records are assigned in round-robin order among the partitions.
    private static MockConsumer<byte[], byte[]> mkMockConsumer(
            List<String> topics,
            int partitionsPerTopic,
            int numElements,
            OffsetResetStrategy offsetResetStrategy,
            Map<String, Object> config,
            SerializableFunction<Integer, byte[]> keyFunction,
            SerializableFunction<Integer, byte[]> valueFunction) {

        final List<TopicPartition> partitions = new ArrayList<>();
        final Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> records = new HashMap<>();
        Map<String, List<PartitionInfo>> partitionMap = new HashMap<>();

        for (String topic : topics) {
            List<PartitionInfo> partIds = new ArrayList<>(partitionsPerTopic);
            for (int i = 0; i < partitionsPerTopic; i++) {
                TopicPartition tp = new TopicPartition(topic, i);
                partitions.add(tp);
                partIds.add(new PartitionInfo(topic, i, null, null, null));
                records.put(tp, new ArrayList<>());
            }
            partitionMap.put(topic, partIds);
        }

        int numPartitions = partitions.size();
        final long[] offsets = new long[numPartitions];

        long timestampStartMillis =
                (Long)
                        config.getOrDefault(TIMESTAMP_START_MILLIS_CONFIG, LOG_APPEND_START_TIME.getMillis());
        TimestampType timestampType =
                TimestampType.forName(
                        (String)
                                config.getOrDefault(
                                        TIMESTAMP_TYPE_CONFIG, TimestampType.LOG_APPEND_TIME.toString()));

        for (int i = 0; i < numElements; i++) {
            int pIdx = i % numPartitions;
            TopicPartition tp = partitions.get(pIdx);

            byte[] key = keyFunction.apply(i);
            byte[] value = valueFunction.apply(i);

            records
                    .get(tp)
                    .add(
                            new ConsumerRecord<>(
                                    tp.topic(),
                                    tp.partition(),
                                    offsets[pIdx]++,
                                    timestampStartMillis + Duration.standardSeconds(i).getMillis(),
                                    timestampType,
                                    0,
                                    key.length,
                                    value.length,
                                    key,
                                    value));
        }

        // This is updated when reader assigns partitions.
        final AtomicReference<List<TopicPartition>> assignedPartitions =
                new AtomicReference<>(Collections.emptyList());

        final MockConsumer<byte[], byte[]> consumer =
                new MockConsumer<byte[], byte[]>(offsetResetStrategy) {
                    @Override
                    public synchronized void assign(final Collection<TopicPartition> assigned) {
                        super.assign(assigned);
                        assignedPartitions.set(ImmutableList.copyOf(assigned));
                        for (TopicPartition tp : assigned) {
                            updateBeginningOffsets(ImmutableMap.of(tp, 0L));
                            updateEndOffsets(ImmutableMap.of(tp, (long) records.get(tp).size()));
                        }
                    }
                    // Override offsetsForTimes() in order to look up the offsets by timestamp.
                    @Override
                    public synchronized Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes(
                            Map<TopicPartition, Long> timestampsToSearch) {
                        return timestampsToSearch.entrySet().stream()
                                .map(
                                        e -> {
                                            // In test scope, timestamp == offset.
                                            long maxOffset = offsets[partitions.indexOf(e.getKey())];
                                            long offset = e.getValue();
                                            OffsetAndTimestamp value =
                                                    (offset >= maxOffset) ? null : new OffsetAndTimestamp(offset, offset);
                                            return new SimpleEntry<>(e.getKey(), value);
                                        })
                                .collect(Collectors.toMap(SimpleEntry::getKey, SimpleEntry::getValue));
                    }
                };

        for (String topic : topics) {
            consumer.updatePartitions(topic, partitionMap.get(topic));
        }

        // MockConsumer does not maintain any relationship between partition seek position and the
        // records added. e.g. if we add 10 records to a partition and then seek to end of the
        // partition, MockConsumer is still going to return the 10 records in next poll. It is
        // our responsibility to make sure currently enqueued records sync with partition offsets.
        // The following task will be called inside each invocation to MockConsumer.poll().
        // We enqueue only the records with the offset >= partition's current position.
        Runnable recordEnqueueTask =
                new Runnable() {
                    @Override
                    public void run() {
                        // add all the records with offset >= current partition position.
                        int recordsAdded = 0;
                        for (TopicPartition tp : assignedPartitions.get()) {
                            long curPos = consumer.position(tp);
                            for (ConsumerRecord<byte[], byte[]> r : records.get(tp)) {
                                if (r.offset() >= curPos) {
                                    consumer.addRecord(r);
                                    recordsAdded++;
                                }
                            }
                        }
                        if (recordsAdded == 0) {
                            if (config.get("inject.error.at.eof") != null) {
                                consumer.setPollException(new KafkaException("Injected error in consumer.poll()"));
                            }
                            // MockConsumer.poll(timeout) does not actually wait even when there aren't any
                            // records.
                            // Add a small wait here in order to avoid busy looping in the reader.
                            Uninterruptibles.sleepUninterruptibly(10, TimeUnit.MILLISECONDS);
                            // TODO: BEAM-4086: testUnboundedSourceWithoutBoundedWrapper() occasionally hangs
                            //     without this wait. Need to look into it.
                        }
                        consumer.schedulePollTask(this);
                    }
                };

        consumer.schedulePollTask(recordEnqueueTask);
        return consumer;
    }

    private static class ConsumerFactoryFn
            implements SerializableFunction<Map<String, Object>, Consumer<byte[], byte[]>> {
        private final List<String> topics;
        private final int partitionsPerTopic;
        private final int numElements;
        private final OffsetResetStrategy offsetResetStrategy;
        private final SerializableFunction<Integer, byte[]> keyFunction;
        private final SerializableFunction<Integer, byte[]> valueFunction;

        @SuppressWarnings("SameParameterValue")
        ConsumerFactoryFn(
                List<String> topics,
                int partitionsPerTopic,
                int numElements,
                OffsetResetStrategy offsetResetStrategy,
                byte[][] values) {
            this.topics = topics;
            this.partitionsPerTopic = partitionsPerTopic;
            this.numElements = numElements;
            this.offsetResetStrategy = offsetResetStrategy;
            this.keyFunction = i -> ByteBuffer.wrap(new byte[8]).putLong(i).array();
            if(values.length == 1)
                this.valueFunction = i -> values[0];
            else
                this.valueFunction = i -> i % 2 == 0 ? values[0] : values[1];
        }

        @Override
        public Consumer<byte[], byte[]> apply(Map<String, Object> config) {
            return mkMockConsumer(
                    topics,
                    partitionsPerTopic,
                    numElements,
                    offsetResetStrategy,
                    config,
                    keyFunction,
                    valueFunction);
        }
    }
    /*
    Mocking Kafka Brokers End
     */


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

        Map<String,Object> config = new HashMap<>();
        config.put("schema.registry.url","mock://");
        LeapAvroSerializer serializer = new LeapAvroSerializer(new LeapSerializerConfig(config));
        AvroDeserializer<Object> deserializer = new LeapAvroDeserializer(new LeapSerializerConfig(config));
        serializedGenericAccountCreatedRecord = serializer.serialize("account-created",
                AvroTestDataProvider.genericRecordDataEventAccountCreated(0L));
        serializedGenericAccountBalanceUpdatedRecord = serializer.serialize("balance-updated",
                AvroTestDataProvider.genericRecordDataEventAccountBalanceUpdated(1L));

        PipelineOptionsFactory.register(KafkaPipelineOptions.class);
    }

    @Test
    public void testKafkaIOReadGeneric(){
        List<String> topics = ImmutableList.of("generic-topic");
        Integer numElements = 100;
        byte[][] values = new byte[1][];
        values[0] = serializedGenericAccountCreatedRecord;
        PCollection<Long> eventIds = p.apply(LeapKafkaIO.readGeneric()
                .withTopic("generic-topic")
                .withSchemaName("account-created")
                .withConsumerFactoryFn(new ConsumerFactoryFn(
                        topics, 10, numElements, OffsetResetStrategy.EARLIEST,values))
                .withMaxNumRecords(numElements.longValue())
        ).apply(ParDo.of(extractEventIdDoFn()));
        //All of the elements has 0 as eventId
        //Only one unique record exist, min and max is 0
        addCountingAsserts(eventIds, numElements, 1L,0L,0L);
        p.run();
    }

    @Test
    public void testKafkaIOReadGenericWithReaderSchema(){
        List<String> topics = ImmutableList.of("generic-topic");
        Integer numElements = 100;
        byte[][] values = new byte[2][];
        values[0] = serializedGenericAccountCreatedRecord;
        values[1] = serializedGenericAccountBalanceUpdatedRecord;
        PCollection<Long> eventIds = p.apply(LeapKafkaIO.readGeneric()
                .withTopic("generic-topic")
                .withConsumerFactoryFn(new ConsumerFactoryFn(
                        topics, 10, numElements, OffsetResetStrategy.EARLIEST,values
                        ))
                .withMaxNumRecords(numElements.longValue())
                .withReaderSchema(AvroTestDataProvider.AVRO_READER_SCHEMA_EVENT_ACCOUNT)
        ).apply(ParDo.of(extractEventIdDoFn()));
        //Two unique element with different write schema
        addCountingAsserts(eventIds, numElements, 2L,0L,1L);
        p.run();
    }

    private static DoFn<KafkaRecord<Long, GenericRecord>, Long> extractEventIdDoFn() {
        return new DoFn<KafkaRecord<Long, GenericRecord>, Long>() {
            @ProcessElement
            public void processElement(@Element KafkaRecord<Long, GenericRecord> element, ProcessContext c) {
                c.output((Long)element.getKV().getValue().get("eventId"));
            }
        };
    }
}
